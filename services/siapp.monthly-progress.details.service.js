// services/siapp.monthly-progress.details.service.js
import pool from "../config/database.js";

function normalizePeriodInput(period) {
  if (Array.isArray(period)) return period[0];
  if (period === null || period === undefined) return null;
  return String(period).trim();
}

function parsePeriod(period) {
  const p = normalizePeriodInput(period);
  if (!p) return null;

  const m = p.match(/^(\d{4})-(\d{1,2})$/);
  if (!m) return null;

  const year = Number(m[1]);
  const month = Number(m[2]);

  if (!Number.isFinite(year) || !Number.isFinite(month)) return null;
  if (month < 1 || month > 12) return null;

  return { year, month };
}

function normTxt(v) {
  if (v == null) return "";
  return String(v)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
}

function normalizeDistrictMode(mode) {
  const m = String(mode || "auto").toLowerCase().trim();
  if (m === "district" || m === "district_claro" || m === "auto") return m;
  return "auto";
}

async function resolveUser(client, { user_id, advisor_id }) {
  const uid = user_id != null && String(user_id).trim() !== "" ? Number(user_id) : null;
  const aid = advisor_id != null && String(advisor_id).trim() !== "" ? String(advisor_id).trim() : null;

  if (!uid && !aid) {
    const err = new Error("Debes enviar user_id o advisor_id (document_id).");
    err.status = 400;
    throw err;
  }

  if (uid) {
    const { rows } = await client.query(
      `
      SELECT id, document_id, name, email, phone, role, district, district_claro, active
      FROM core.users
      WHERE id = $1
      LIMIT 1
      `,
      [uid]
    );
    if (!rows[0]) {
      const err = new Error("No existe el usuario para ese user_id.");
      err.status = 404;
      throw err;
    }
    return rows[0];
  }

  const { rows } = await client.query(
    `
    SELECT id, document_id, name, email, phone, role, district, district_claro, active
    FROM core.users
    WHERE document_id::text = $1::text
    LIMIT 1
    `,
    [aid]
  );

  if (!rows[0]) {
    const err = new Error("No existe el usuario en core.users para ese advisor_id/document_id.");
    err.status = 404;
    throw err;
  }

  return rows[0];
}

/**
 * Detalle para “click asesor”:
 * - user info (core.users)
 * - progress del mes (core.progress)
 * - budget oficial (core.user_monthly)
 * - sales summary IN/OUT (desde siapp.full_sales) según district_mode
 *   * KPI: por FILAS
 *   * cantserv: analítica separada (services_live)
 * - top distritos OUT para debug
 * - links: sales detail + sales summary (por consistencia front)
 */
export async function getMonthlyProgressDetails({
  period,
  user_id = null,
  advisor_id = null,        // document_id
  district_mode = "auto"
} = {}) {
  const per = parsePeriod(period);
  if (!per) throw new Error("Falta o es inválido ?period=YYYY-MM");

  const { year, month } = per;
  const mode = normalizeDistrictMode(district_mode);

  const client = await pool.connect();
  try {
    // 1) Resolver usuario (de nómina/presupuesto)
    const u = await resolveUser(client, { user_id, advisor_id });

    const advisorId = String(u.document_id || "").trim();
    if (!advisorId) {
      const err = new Error("El usuario no tiene document_id; no se puede linkear con SIAPP (idasesor).");
      err.status = 400;
      throw err;
    }

    // 2) Traer progress (si existe)
    const { rows: pRows } = await client.query(
      `
      SELECT
        user_id, period_year, period_month,
        real_in_count, real_out_count, real_total_count,
        expected_count, adjusted_count,
        compliance_in_percent, compliance_global_percent,
        met_in_district, met_global,
        created_at, updated_at
      FROM core.progress
      WHERE user_id = $1 AND period_year = $2 AND period_month = $3
      LIMIT 1
      `,
      [u.id, year, month]
    );

    const progress = pRows[0] || null;

    // 3) Traer user_monthly (si existe)
    const { rows: umRows } = await client.query(
      `
      SELECT presupuesto_mes, dias_laborados, prorrateo, updated_at
      FROM core.user_monthly
      WHERE user_id = $1 AND period_year = $2 AND period_month = $3
      LIMIT 1
      `,
      [u.id, year, month]
    );
    const um = umRows[0] || null;

    // Distrito usuario (igual que tu lógica actual)
    const distritoUsuario =
      mode === "district"
        ? (u.district ?? null)
        : mode === "district_claro"
          ? (u.district_claro ?? null)
          : (u.district_claro || u.district || null);

    const distritoUsuarioParam =
      distritoUsuario != null && String(distritoUsuario).trim() !== ""
        ? String(distritoUsuario).trim()
        : null;

    // 4) LIVE desde full_sales:
    //    - sales_live: por FILAS (conteo)
    //    - services_live: cantserv (analítica)
    const { rows: liveRows } = await client.query(
      `
      WITH base AS (
        SELECT
          fs.d_distrito AS distrito_venta,
          COALESCE(
            NULLIF( (regexp_match(COALESCE(fs.cantserv::text,''), '(-?\\d+(?:[\\.,]\\d+)?)'))[1], '' ),
            '0'
          ) AS cant_str
        FROM siapp.full_sales fs
        WHERE fs.period_year = $1
          AND fs.period_month = $2
          AND fs.idasesor::text = $3::text
      ),
      parsed AS (
        SELECT
          distrito_venta,
          REPLACE(cant_str, ',', '.')::numeric AS cant
        FROM base
      )
      SELECT
        COUNT(*)::int AS total_rows,

        -- KPI: por FILAS (conteo)
        SUM(
          CASE
            WHEN $4::text IS NOT NULL
             AND distrito_venta IS NOT NULL
             AND distrito_venta = $4::text
            THEN 1 ELSE 0 END
        )::int AS in_rows,

        SUM(
          CASE
            WHEN NOT (
              $4::text IS NOT NULL
              AND distrito_venta IS NOT NULL
              AND distrito_venta = $4::text
            )
            THEN 1 ELSE 0 END
        )::int AS out_rows,

        -- Analítica: cantserv (NO KPI)
        COALESCE(SUM(cant),0)::numeric AS cantserv_total,

        COALESCE(SUM(
          CASE
            WHEN $4::text IS NOT NULL
             AND distrito_venta IS NOT NULL
             AND distrito_venta = $4::text
            THEN cant ELSE 0 END
        ),0)::numeric AS cantserv_in,

        COALESCE(SUM(
          CASE
            WHEN NOT (
              $4::text IS NOT NULL
              AND distrito_venta IS NOT NULL
              AND distrito_venta = $4::text
            )
            THEN cant ELSE 0 END
        ),0)::numeric AS cantserv_out

      FROM parsed
      `,
      [year, month, advisorId, distritoUsuarioParam]
    );

    const live = liveRows[0] || {};

    // KPI live por filas
    const total_rows = Number(live.total_rows || 0);
    const real_in_rows = Number(live.in_rows || 0);
    const real_out_rows = Number(live.out_rows || 0);

    // Analítica live cantserv
    const cantserv_total = Number(live.cantserv_total || 0);
    const cantserv_in = Number(live.cantserv_in || 0);
    const cantserv_out = Number(live.cantserv_out || 0);

    // 5) Top OUT distritos (debug / comparación)
    // Mantengo tu estructura (rows + cantidad). Aquí cantidad sigue siendo cantserv (analítica),
    // y rows son filas out.
    const { rows: topOutRows } = await client.query(
      `
      SELECT
        fs.d_distrito AS distrito,
        COUNT(*)::int AS rows,
        COALESCE(SUM(
          REPLACE(
            COALESCE(NULLIF((regexp_match(COALESCE(fs.cantserv::text,''), '(-?\\d+(?:[\\.,]\\d+)?)'))[1], ''), '0'),
            ',', '.'
          )::numeric
        ),0)::numeric AS cantidad
      FROM siapp.full_sales fs
      WHERE fs.period_year = $1
        AND fs.period_month = $2
        AND fs.idasesor::text = $3::text
        AND (
          $4::text IS NULL
          OR fs.d_distrito IS NULL
          OR fs.d_distrito <> $4::text
        )
      GROUP BY 1
      ORDER BY cantidad DESC, rows DESC
      LIMIT 10
      `,
      [year, month, advisorId, distritoUsuarioParam]
    );

    const top_out_districts = topOutRows.map(r => ({
      distrito: r.distrito ?? null,
      rows: Number(r.rows || 0),
      cantidad: Number(r.cantidad || 0) // cantserv analítica
    }));

    // 6) Links útiles para el front (NO CAMBIO tu periodStr)
    const periodStr = `${year}-${String(month).padStart(2, "0")}`;

    return {
      period: periodStr,
      period_year: year,
      period_month: month,
      district_mode: mode,

      advisor: {
        user_id: String(u.id),
        advisor_id: advisorId, // document_id
        name: u.name ?? null,
        email: u.email ?? null,
        phone: u.phone ?? null,
        role: u.role ?? null,
        active: u.active ?? null,
        district: u.district ?? null,
        district_claro: u.district_claro ?? null,
        distrito_usuario: distritoUsuario
      },

      // Fuente “oficial” ya calculada por promote (si existe)
      progress: progress
        ? {
            // IMPORTANTE: ahora estos counts deben representar FILAS (cuando recalcules promote)
            real_in: Number(progress.real_in_count || 0),
            real_out: Number(progress.real_out_count || 0),
            real_total: Number(progress.real_total_count || 0),

            expected: Number(progress.expected_count || 0),
            adjusted: Number(progress.adjusted_count || 0),

            compliance_in_percent: Number(progress.compliance_in_percent || 0),
            compliance_global_percent: Number(progress.compliance_global_percent || 0),

            met_in_district: !!progress.met_in_district,
            met_global: !!progress.met_global,

            created_at: progress.created_at ?? null,
            updated_at: progress.updated_at ?? null
          }
        : null,

      // LIVE KPI (FILAS) desde full_sales
      sales_live: {
        total_rows,
        real_in: real_in_rows,
        real_out: real_out_rows,
        real_total: total_rows
      },

      // LIVE analítica cantserv (NO KPI)
      services_live: {
        cantserv_in,
        cantserv_out,
        cantserv_total
      },

      budget: um
        ? {
            contracted_month: true,
            presupuesto_mes: um.presupuesto_mes != null ? Number(um.presupuesto_mes) : null,
            dias_laborados: um.dias_laborados != null ? Number(um.dias_laborados) : null,
            prorrateo: um.prorrateo != null ? Number(um.prorrateo) : null,
            updated_at: um.updated_at ?? null
          }
        : {
            contracted_month: false,
            presupuesto_mes: null,
            dias_laborados: null,
            prorrateo: null,
            updated_at: null
          },

      comparisons: {
        distrito_usuario_norm: distritoUsuario ? normTxt(distritoUsuario) : null,
        note:
          "KPI/metas usan FILAS (conexiones). cantserv se expone aparte como analítica en services_live. La comparación principal usa igualdad exacta en BD (district_claro recomendado)."
      },

      top_out_districts,

      links: {
        sales_summary: `/api/siapp/monthly/sales/summary?period=${periodStr}&advisor_id=${encodeURIComponent(advisorId)}&district_mode=${encodeURIComponent(mode)}`,
        sales_detail: `/api/siapp/monthly/sales?period=${periodStr}&advisor_id=${encodeURIComponent(advisorId)}&district_mode=${encodeURIComponent(mode)}`,
        progress_summary: `/api/siapp/monthly/progress/summary?period=${periodStr}`
      }
    };
  } finally {
    client.release();
  }
}

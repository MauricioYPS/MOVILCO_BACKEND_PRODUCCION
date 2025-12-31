// services/siapp.monthly-progress.summary.service.js
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

function clampInt(n, min, max, fallback) {
  const x = Number(n);
  if (!Number.isFinite(x)) return fallback;
  return Math.min(Math.max(Math.trunc(x), min), max);
}

function parseBool(v) {
  if (v === true || v === "true" || v === 1 || v === "1") return true;
  if (v === false || v === "false" || v === 0 || v === "0") return false;
  return null;
}

export async function getMonthlyProgressSummary({
  period,
  limit = 200,
  offset = 0,
  q = null,
  // filtros opcionales
  only_met_in = null,       // true/false
  only_met_global = null,   // true/false
  only_contracted = null,   // true/false (si existe core.user_monthly para el mes)
  only_in_payroll = null    // true/false (si existe core.users)
} = {}) {
  const per = parsePeriod(period);
  if (!per) throw new Error("Falta o es inválido ?period=YYYY-MM");

  const { year, month } = per;

  const safeLimit = clampInt(limit, 1, 2000, 200);
  const safeOffset = clampInt(offset, 0, 10_000_000, 0);

  const hasQ = q && String(q).trim().length > 0;
  const qLike = hasQ ? `%${String(q).trim().toUpperCase()}%` : null;

  const metIn = parseBool(only_met_in);
  const metGlobal = parseBool(only_met_global);
  const contracted = parseBool(only_contracted);
  const inPayroll = parseBool(only_in_payroll);

  // -------------------------------------------------------------------
  // BASE: core.progress + core.users + core.user_monthly
  //  - core.progress es la fuente de KPI del mes (ya recalculado desde SIAPP)
  //  - core.user_monthly nos permite saber contratado_mes y mostrar presupuesto/prorrateo oficiales
  // -------------------------------------------------------------------
  const where = `
    p.period_year = $1
    AND p.period_month = $2

    AND (
      $3::text IS NULL OR
      UPPER(COALESCE(u.document_id::text,'')) LIKE $3 OR
      UPPER(COALESCE(u.name,'')) LIKE $3 OR
      UPPER(COALESCE(u.email,'')) LIKE $3 OR
      UPPER(COALESCE(u.phone::text,'')) LIKE $3 OR
      UPPER(COALESCE(u.district_claro,'')) LIKE $3 OR
      UPPER(COALESCE(u.district,'')) LIKE $3
    )

    AND ($4::bool IS NULL OR p.met_in_district = $4)
    AND ($5::bool IS NULL OR p.met_global = $5)

    -- contratado_mes: existe fila en core.user_monthly para el periodo
    AND (
      $6::bool IS NULL OR
      ($6 = true  AND um.user_id IS NOT NULL) OR
      ($6 = false AND um.user_id IS NULL)
    )

    -- en_nomina: existe core.users (siempre debería para progress, pero lo dejamos)
    AND (
      $7::bool IS NULL OR
      ($7 = true  AND u.id IS NOT NULL) OR
      ($7 = false AND u.id IS NULL)
    )
  `;

  // -------------------------------------------------------------------
  // TOTAL
  // -------------------------------------------------------------------
  const totalQ = await pool.query(
    `
    SELECT COUNT(*)::int AS total
    FROM core.progress p
    LEFT JOIN core.users u ON u.id = p.user_id
    LEFT JOIN core.user_monthly um
      ON um.user_id = p.user_id
     AND um.period_year = p.period_year
     AND um.period_month = p.period_month
    WHERE ${where}
    `,
    [year, month, qLike, metIn, metGlobal, contracted, inPayroll]
  );

  const total = Number(totalQ.rows[0]?.total || 0);

  // -------------------------------------------------------------------
  // DATA
  // -------------------------------------------------------------------
  const rowsQ = await pool.query(
    `
    SELECT
      p.user_id,
      p.period_year,
      p.period_month,

      p.real_in_count,
      p.real_out_count,
      p.real_total_count,

      p.expected_count,
      p.adjusted_count,

      p.compliance_in_percent,
      p.compliance_global_percent,

      p.met_in_district,
      p.met_global,

      p.created_at,
      p.updated_at,

      u.document_id,
      u.name,
      u.email,
      u.phone,
      u.district,
      u.district_claro,
      u.role,

      -- contratado_mes si hay core.user_monthly
      (um.user_id IS NOT NULL) AS contracted_month,
      um.presupuesto_mes,
      um.dias_laborados,
      um.prorrateo

    FROM core.progress p
    LEFT JOIN core.users u ON u.id = p.user_id
    LEFT JOIN core.user_monthly um
      ON um.user_id = p.user_id
     AND um.period_year = p.period_year
     AND um.period_month = p.period_month

    WHERE ${where}

    ORDER BY
      -- primero los que no cumplen (para revisar)
      p.met_global ASC,
      p.compliance_global_percent ASC,
      u.name ASC NULLS LAST,
      p.user_id ASC
    LIMIT $8 OFFSET $9
    `,
    [year, month, qLike, metIn, metGlobal, contracted, inPayroll, safeLimit, safeOffset]
  );

  const rows = rowsQ.rows.map((r) => ({
    user_id: r.user_id,

    period: `${r.period_year}-${String(r.period_month).padStart(2, "0")}`,
    period_year: r.period_year,
    period_month: r.period_month,

    user: {
      id: r.user_id,
      document_id: r.document_id ?? null,
      name: r.name ?? null,
      email: r.email ?? null,
      phone: r.phone ?? null,
      role: r.role ?? null,
      district: r.district ?? null,
      district_claro: r.district_claro ?? null
    },

    contracted_month: !!r.contracted_month,

    sales: {
      real_in: Number(r.real_in_count || 0),
      real_out: Number(r.real_out_count || 0),
      real_total: Number(r.real_total_count || 0)
    },

    budget: {
      expected: Number(r.expected_count || 0),
      adjusted: Number(r.adjusted_count || 0),

      // Estos tres vienen de user_monthly si existe (más “fuente de verdad”)
      presupuesto_mes: r.presupuesto_mes != null ? Number(r.presupuesto_mes) : null,
      dias_laborados: r.dias_laborados != null ? Number(r.dias_laborados) : null,
      prorrateo: r.prorrateo != null ? Number(r.prorrateo) : null
    },

    compliance: {
      in_percent: r.compliance_in_percent != null ? Number(r.compliance_in_percent) : 0,
      global_percent: r.compliance_global_percent != null ? Number(r.compliance_global_percent) : 0
    },

    flags: {
      met_in_district: !!r.met_in_district,
      met_global: !!r.met_global
    },

    timestamps: {
      created_at: r.created_at ?? null,
      updated_at: r.updated_at ?? null
    }
  }));

  // -------------------------------------------------------------------
  // AGREGADOS RÁPIDOS (para cards en UI)
  // -------------------------------------------------------------------
  const aggQ = await pool.query(
    `
    SELECT
      COUNT(*)::int AS total_rows,
      SUM(CASE WHEN p.met_global THEN 1 ELSE 0 END)::int AS met_global_yes,
      SUM(CASE WHEN p.met_in_district THEN 1 ELSE 0 END)::int AS met_in_yes,
      AVG(COALESCE(p.compliance_global_percent,0))::numeric(12,2) AS avg_global,
      AVG(COALESCE(p.compliance_in_percent,0))::numeric(12,2) AS avg_in
    FROM core.progress p
    LEFT JOIN core.users u ON u.id = p.user_id
    LEFT JOIN core.user_monthly um
      ON um.user_id = p.user_id
     AND um.period_year = p.period_year
     AND um.period_month = p.period_month
    WHERE ${where}
    `,
    [year, month, qLike, metIn, metGlobal, contracted, inPayroll]
  );

  const agg = aggQ.rows[0] || {};

  return {
    period: `${year}-${String(month).padStart(2, "0")}`,
    period_year: year,
    period_month: month,

    total,
    limit: safeLimit,
    offset: safeOffset,

    aggregates: {
      total_rows: Number(agg.total_rows || 0),
      met_global_yes: Number(agg.met_global_yes || 0),
      met_in_yes: Number(agg.met_in_yes || 0),
      avg_global: Number(agg.avg_global || 0),
      avg_in: Number(agg.avg_in || 0)
    },

    rows
  };
}

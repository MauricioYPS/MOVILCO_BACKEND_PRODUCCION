// services/novedades.manual.service.js
import pool from "../config/database.js";
import { promoteSiappFromFullSales } from "./promote.siapp.service.js";

/**
 * Utilidades
 */
function toISODateOnly(v) {
  if (!v) return null;
  const d = new Date(v);
  if (isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10); // YYYY-MM-DD
}

function monthsBetween(startISO, endISO) {
  const start = new Date(`${startISO}T00:00:00Z`);
  const end = new Date(`${endISO}T00:00:00Z`);
  if (end < start) return [];

  const out = [];
  let y = start.getUTCFullYear();
  let m = start.getUTCMonth() + 1; // 1..12

  const endY = end.getUTCFullYear();
  const endM = end.getUTCMonth() + 1;

  while (y < endY || (y === endY && m <= endM)) {
    out.push({ year: y, month: m });
    m++;
    if (m === 13) {
      m = 1;
      y++;
    }
  }
  return out;
}

async function resolveUserId(client, { user_id, document_id, name }) {
  if (user_id) {
    const { rows } = await client.query(
      `SELECT id FROM core.users WHERE id = $1 LIMIT 1`,
      [Number(user_id)]
    );
    return rows[0]?.id ?? null;
  }

  if (document_id) {
    const { rows } = await client.query(
      `SELECT id FROM core.users WHERE document_id = $1 LIMIT 1`,
      [String(document_id).trim()]
    );
    return rows[0]?.id ?? null;
  }

  if (name) {
    const { rows } = await client.query(
      `SELECT id FROM core.users WHERE lower(name) = lower($1) LIMIT 1`,
      [String(name).trim()]
    );
    return rows[0]?.id ?? null;
  }

  return null;
}

/**
 * Detecta solapes de novedades para un usuario en un rango [startISO, endISO].
 * Regla de solape: existing.start_date <= endISO AND existing.end_date >= startISO
 */
async function findOverlappingNovelties(client, userId, startISO, endISO) {
  const { rows } = await client.query(
    `
    SELECT
      id, user_id, novelty_type, start_date, end_date, notes, created_at
    FROM core.user_novelties
    WHERE user_id = $1
      AND start_date <= $3::date
      AND end_date   >= $2::date
    ORDER BY start_date ASC, end_date ASC, id ASC
    `,
    [userId, startISO, endISO]
  );
  return rows;
}

/**
 * Calcula días NO laborados en un mes usando generate_series (dedup de overlaps).
 * Cuenta días cubiertos por cualquier novedad que cruce el mes.
 */
async function countNonWorkedDaysInMonth(client, userId, year, month) {
  const { rows } = await client.query(
    `
    WITH bounds AS (
      SELECT
        make_date($2::int, $3::int, 1) AS month_start,
        (make_date($2::int, $3::int, 1) + interval '1 month' - interval '1 day')::date AS month_end
    ),
    days AS (
      SELECT DISTINCT gs::date AS day
      FROM core.user_novelties n
      CROSS JOIN bounds b
      CROSS JOIN LATERAL generate_series(
        GREATEST(n.start_date, b.month_start),
        LEAST(n.end_date, b.month_end),
        interval '1 day'
      ) gs
      WHERE n.user_id = $1
        AND n.start_date <= b.month_end
        AND n.end_date >= b.month_start
    )
    SELECT COUNT(*)::int AS non_worked
    FROM days
    `,
    [userId, year, month]
  );

  return Number(rows[0]?.non_worked || 0);
}

async function getDaysInMonth(client, year, month) {
  const { rows } = await client.query(
    `
    SELECT EXTRACT(DAY FROM (make_date($1::int,$2::int,1) + interval '1 month' - interval '1 day'))::int AS dim
    `,
    [year, month]
  );
  return Number(rows[0]?.dim || 30);
}

/**
 * Asegura user_monthly y recalcula dias_laborados/prorrateo con regla de 3:
 * prorrateo = presupuesto_mes * (dias_laborados / days_in_month)
 *
 * Si presupuesto_mes es NULL y no existe fila, usamos 13 como fallback.
 */
async function recalcUserMonthlyForPeriod(client, userId, year, month) {
  const daysInMonth = await getDaysInMonth(client, year, month);
  const nonWorked = await countNonWorkedDaysInMonth(client, userId, year, month);

  const diasLaborados = Math.max(daysInMonth - nonWorked, 0);

  // Tomar presupuesto_mes actual (si existe), si no, fallback 13
  const { rows: umRows } = await client.query(
    `
    SELECT presupuesto_mes
    FROM core.user_monthly
    WHERE user_id=$1 AND period_year=$2 AND period_month=$3
    LIMIT 1
    `,
    [userId, year, month]
  );

  const presupuestoMes =
    umRows.length > 0 && umRows[0].presupuesto_mes != null
      ? Number(umRows[0].presupuesto_mes)
      : 13;

  const prorrateo =
    presupuestoMes > 0
      ? Number(((presupuestoMes * diasLaborados) / daysInMonth).toFixed(4))
      : 0;

  await client.query(
    `
    INSERT INTO core.user_monthly (
      user_id, period_year, period_month,
      presupuesto_mes, dias_laborados, prorrateo,
      updated_at
    )
    VALUES ($1,$2,$3,$4,$5,$6, now())
    ON CONFLICT (user_id, period_year, period_month)
    DO UPDATE SET
      -- Si ya existe presupuesto_mes, se respeta. Si no existe, usamos el fallback.
      presupuesto_mes = COALESCE(core.user_monthly.presupuesto_mes, EXCLUDED.presupuesto_mes),
      dias_laborados  = EXCLUDED.dias_laborados,
      prorrateo       = EXCLUDED.prorrateo,
      updated_at      = now()
    `,
    [userId, year, month, presupuestoMes, diasLaborados, prorrateo]
  );

  return {
    period: `${year}-${String(month).padStart(2, "0")}`,
    days_in_month: daysInMonth,
    non_worked_days: nonWorked,
    dias_laborados: diasLaborados,
    presupuesto_mes: presupuestoMes,
    prorrateo
  };
}

/**
 * Recalcula core.progress para los meses tocados
 * (usa el promote SIAPP que ya tienes).
 *
 * IMPORTANTE: esta función asume que promoteSiappFromFullSales
 * abre/cierra su propia transacción/cliente (como en tu implementación).
 */
async function recalcProgressForMonths(months) {
  const results = [];
  let totalUpserted = 0;

  for (const m of months) {
    const r = await promoteSiappFromFullSales({
      period_year: m.year,
      period_month: m.month
    });

    results.push({
      period: `${m.year}-${String(m.month).padStart(2, "0")}`,
      period_year: m.year,
      period_month: m.month,
      total_sales_rows: r.total_sales_rows,
      total_asesores_en_siapp: r.total_asesores_en_siapp,
      matched_users: r.matched_users,
      upserted: r.upserted
    });

    totalUpserted += Number(r.upserted || 0);
  }

  return { total_months: results.length, total_upserted: totalUpserted, results };
}

/**
 * Crea novedad manual + recalcula user_monthly para todos los meses tocados
 * + recalcula core.progress para esos meses.
 *
 * Manejo de conflictos:
 *  - Si hay SOLAPE con otra novedad existente (cualquier tipo): 409
 *  - Si es duplicado EXACTO (unique constraint): 409
 */
export async function createNoveltyManual({
  user_id = null,
  document_id = null,
  name = null,
  novelty_type = "NOVEDAD",
  start_date,
  end_date,
  notes = null
} = {}) {
  const startISO = toISODateOnly(start_date);
  const endISO = toISODateOnly(end_date);

  if (!startISO || !endISO) {
    const err = new Error(
      "start_date y end_date son requeridos (YYYY-MM-DD o fecha válida)."
    );
    err.status = 400;
    throw err;
  }

  if (new Date(`${endISO}T00:00:00Z`) < new Date(`${startISO}T00:00:00Z`)) {
    const err = new Error("end_date no puede ser menor que start_date.");
    err.status = 400;
    throw err;
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const userId = await resolveUserId(client, { user_id, document_id, name });
    if (!userId) {
      const err = new Error("No se encontró el usuario (user_id / document_id / name).");
      err.status = 404;
      throw err;
    }

    // 1) Validación de SOLAPE (cualquier tipo)
    const overlaps = await findOverlappingNovelties(client, userId, startISO, endISO);
    if (overlaps.length > 0) {
      const err = new Error(
        "No se puede crear la novedad: el rango de fechas se solapa con una novedad existente."
      );
      err.status = 409;
      err.code = "NOVELTY_OVERLAP";
      err.overlaps = overlaps;
      throw err;
    }

    // 2) Insertar novedad
    let novelty = null;
    try {
      const { rows: insRows } = await client.query(
        `
        INSERT INTO core.user_novelties (user_id, novelty_type, start_date, end_date, notes)
        VALUES ($1,$2,$3::date,$4::date,$5)
        RETURNING id, user_id, novelty_type, start_date, end_date, notes, created_at
        `,
        [userId, String(novelty_type || "NOVEDAD").trim(), startISO, endISO, notes ?? null]
      );
      novelty = insRows[0];
    } catch (pgErr) {
      // Duplicado exacto por ux_user_novelties_unique
      if (pgErr && pgErr.code === "23505") {
        const err = new Error(
          "La novedad ya existe (mismo tipo y mismo rango de fechas) para este usuario."
        );
        err.status = 409;
        err.code = "NOVELTY_DUPLICATE";
        throw err;
      }
      throw pgErr;
    }

    // 3) Recalcular meses tocados en user_monthly
    const months = monthsBetween(startISO, endISO);
    const monthly_updates = [];
    for (const m of months) {
      const upd = await recalcUserMonthlyForPeriod(client, userId, m.year, m.month);
      monthly_updates.push(upd);
    }

    await client.query("COMMIT");

    // 4) Recalcular core.progress (afuera de la tx de novedades, usando service SIAPP)
    const progress_recalc = await recalcProgressForMonths(months);

    return {
      ok: true,
      novelty,
      monthly_updates,
      progress_recalc
    };
  } catch (e) {
    await client.query("ROLLBACK");

    // Re-empaquetar errores conocidos con status
    if (e && e.status) throw e;

    // Si viene de PG y es 23505 que no capturamos arriba (por otro constraint)
    if (e && e.code === "23505") {
      const err = new Error("Conflicto de unicidad al crear la novedad.");
      err.status = 409;
      err.code = "NOVELTY_UNIQUE_CONFLICT";
      throw err;
    }

    throw e;
  } finally {
    client.release();
  }
}

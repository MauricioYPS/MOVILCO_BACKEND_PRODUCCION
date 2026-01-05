// services/novedades.crud.service.js
import pool from "../config/database.js";
import { promoteSiappFromFullSales } from "./promote.siapp.service.js";

/** ===== util fechas ===== */
function toISODateOnly(v) {
  if (!v) return null;
  const d = new Date(v);
  if (isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function monthsBetween(startISO, endISO) {
  const start = new Date(`${startISO}T00:00:00Z`);
  const end = new Date(`${endISO}T00:00:00Z`);
  if (end < start) return [];

  const out = [];
  let y = start.getUTCFullYear();
  let m = start.getUTCMonth() + 1;
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

function unionMonths(a, b) {
  const key = (x) => `${x.year}-${String(x.month).padStart(2, "0")}`;
  const map = new Map();
  for (const x of a) map.set(key(x), x);
  for (const x of b) map.set(key(x), x);
  return Array.from(map.values()).sort((p, q) => (p.year - q.year) || (p.month - q.month));
}

/** ===== scope por rol ===== */
async function resolveScopeUserIds(client, authUser) {
  const userId = Number(authUser?.id);
  const role = String(authUser?.role || "").toUpperCase();

  if (!userId) {
    const err = new Error("No autenticado");
    err.status = 401;
    throw err;
  }

  // ASESORIA: solo él mismo
  if (role === "ASESORIA") return [userId];

  // COORDINACION: él mismo + asesores que tengan coordinator_id = él
  if (role === "COORDINACION") {
    const { rows } = await client.query(
      `
      SELECT id
      FROM core.users
      WHERE id = $1
         OR coordinator_id = $1
      `,
      [userId]
    );
    return rows.map((r) => Number(r.id));
  }

  // DIRECCION/GERENCIA/otros admin: por ahora permitimos todo
  const { rows } = await client.query(`SELECT id FROM core.users`);
  return rows.map((r) => Number(r.id));
}

async function ensureNoveltyInScope(client, authUser, noveltyId) {
  const scopeIds = await resolveScopeUserIds(client, authUser);

  const { rows } = await client.query(
    `
    SELECT id, user_id, novelty_type, start_date, end_date, notes, created_at, updated_at
    FROM core.user_novelties
    WHERE id = $1
    LIMIT 1
    `,
    [Number(noveltyId)]
  );

  const n = rows[0];
  if (!n) {
    const err = new Error("Novedad no encontrada");
    err.status = 404;
    throw err;
  }

  if (!scopeIds.includes(Number(n.user_id))) {
    const err = new Error("Acceso denegado");
    err.status = 403;
    throw err;
  }

  return { novelty: n, scopeIds };
}

/** ===== cálculo de días ===== */
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
 * Presupuesto base (source of truth): core.budgets
 * - period: 'YYYY-MM'
 * - budget_amount: entero (numeric(14,0))
 * - status: idealmente ACTIVE (ya lo migraste)
 */
async function getBaseBudgetFromBudgets(client, userId, periodStr) {
  const { rows } = await client.query(
    `
    SELECT b.budget_amount
    FROM core.budgets b
    WHERE b.user_id = $1
      AND b.period = $2
      AND (b.status = 'ACTIVE' OR b.status IS NULL)
    ORDER BY b.updated_at DESC NULLS LAST, b.id DESC
    LIMIT 1
    `,
    [userId, periodStr]
  );

  if (!rows[0] || rows[0].budget_amount == null) return null;
  const n = Number(rows[0].budget_amount);
  return Number.isFinite(n) ? n : null;
}

async function recalcUserMonthlyForPeriod(client, userId, year, month) {
  const daysInMonth = await getDaysInMonth(client, year, month);
  const nonWorked = await countNonWorkedDaysInMonth(client, userId, year, month);
  const diasLaborados = Math.max(daysInMonth - nonWorked, 0);

  const periodStr = `${year}-${String(month).padStart(2, "0")}`;

  // 1) Source of truth: core.budgets (presupuesto base)
  const budgetBase = await getBaseBudgetFromBudgets(client, userId, periodStr);

  // 2) Si no existe budget, conservamos el valor anterior de user_monthly (si existe), si no fallback 13
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
    budgetBase != null
      ? budgetBase
      : (umRows.length > 0 && umRows[0].presupuesto_mes != null
          ? Number(umRows[0].presupuesto_mes)
          : 13);

  const prorrateo =
    presupuestoMes > 0
      ? Number(((presupuestoMes * diasLaborados) / daysInMonth).toFixed(4))
      : 0;

  // Guardar mensual (presupuesto_mes es base del mes; dias/prorrateo dependen de novedades)
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
      presupuesto_mes = EXCLUDED.presupuesto_mes,
      dias_laborados  = EXCLUDED.dias_laborados,
      prorrateo       = EXCLUDED.prorrateo,
      updated_at      = now()
    `,
    [userId, year, month, presupuestoMes, diasLaborados, prorrateo]
  );

  return {
    period: periodStr,
    days_in_month: daysInMonth,
    non_worked_days: nonWorked,
    dias_laborados: diasLaborados,
    presupuesto_mes: presupuestoMes,
    prorrateo
  };
}

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
      upserted: r.upserted,
      matched_users: r.matched_users,
      total_sales_rows: r.total_sales_rows
    });

    totalUpserted += Number(r.upserted || 0);
  }

  return { total_months: results.length, total_upserted: totalUpserted, results };
}

/** ===== LIST ===== */
export async function listNovelties({ authUser, date_from, date_to, q, limit = 50, offset = 0 }) {
  const client = await pool.connect();
  try {
    const scopeIds = await resolveScopeUserIds(client, authUser);

    const df = date_from ? toISODateOnly(date_from) : null;
    const dt = date_to ? toISODateOnly(date_to) : null;
    const qq = q != null ? String(q) : null;

    const { rows: totalRows } = await client.query(
      `
      SELECT COUNT(*)::int AS total
      FROM core.user_novelties n
      JOIN core.users u ON u.id = n.user_id
      WHERE u.id = ANY($1::int[])
        AND (
          ($2::date IS NULL AND $3::date IS NULL)
          OR (n.start_date <= COALESCE($3::date, n.end_date)
              AND n.end_date >= COALESCE($2::date, n.start_date))
        )
        AND (
          $4::text IS NULL OR btrim($4::text) = ''
          OR lower(u.name) LIKE lower('%'||$4||'%')
          OR u.document_id LIKE '%'||$4||'%'
          OR lower(COALESCE(n.notes,'')) LIKE lower('%'||$4||'%')
          OR lower(COALESCE(n.novelty_type,'')) LIKE lower('%'||$4||'%')
        )
      `,
      [scopeIds, df, dt, qq]
    );

    const total = Number(totalRows[0]?.total || 0);

    const { rows: items } = await client.query(
      `
      SELECT
        n.id, n.user_id, n.novelty_type, n.start_date, n.end_date, n.notes, n.created_at, n.updated_at,
        u.name AS user_name, u.document_id, u.role, u.regional, u.district, u.district_claro
      FROM core.user_novelties n
      JOIN core.users u ON u.id = n.user_id
      WHERE u.id = ANY($1::int[])
        AND (
          ($2::date IS NULL AND $3::date IS NULL)
          OR (n.start_date <= COALESCE($3::date, n.end_date)
              AND n.end_date >= COALESCE($2::date, n.start_date))
        )
        AND (
          $4::text IS NULL OR btrim($4::text) = ''
          OR lower(u.name) LIKE lower('%'||$4||'%')
          OR u.document_id LIKE '%'||$4||'%'
          OR lower(COALESCE(n.notes,'')) LIKE lower('%'||$4||'%')
          OR lower(COALESCE(n.novelty_type,'')) LIKE lower('%'||$4||'%')
        )
      ORDER BY n.created_at DESC, n.id DESC
      LIMIT $5 OFFSET $6
      `,
      [scopeIds, df, dt, qq, limit, offset]
    );

    return { ok: true, total, limit, offset, items };
  } finally {
    client.release();
  }
}

/** ===== RECENT ===== */
export async function listRecentNovelties({ authUser, days = 3, limit = 50 }) {
  const client = await pool.connect();
  try {
    const scopeIds = await resolveScopeUserIds(client, authUser);

    const { rows: items } = await client.query(
      `
      SELECT
        n.id, n.user_id, n.novelty_type, n.start_date, n.end_date, n.notes, n.created_at, n.updated_at,
        u.name AS user_name, u.document_id, u.role, u.district_claro, u.district
      FROM core.user_novelties n
      JOIN core.users u ON u.id = n.user_id
      WHERE u.id = ANY($1::int[])
        AND n.created_at >= (now() - make_interval(days => $2::int))
      ORDER BY n.created_at DESC, n.id DESC
      LIMIT $3
      `,
      [scopeIds, days, limit]
    );

    return { ok: true, days, limit, items };
  } finally {
    client.release();
  }
}

/** ===== DETAIL ===== */
export async function getNoveltyById({ authUser, id }) {
  const client = await pool.connect();
  try {
    const { novelty } = await ensureNoveltyInScope(client, authUser, id);

    const { rows: extra } = await client.query(
      `
      SELECT
        u.name AS user_name, u.document_id, u.role, u.regional, u.district, u.district_claro
      FROM core.users u
      WHERE u.id = $1
      LIMIT 1
      `,
      [novelty.user_id]
    );

    return { ok: true, novelty: { ...novelty, ...extra[0] } };
  } finally {
    client.release();
  }
}

/** ===== UPDATE ===== */
async function findOverlapsExcludingId(client, userId, startISO, endISO, excludeId) {
  const { rows } = await client.query(
    `
    SELECT id, user_id, novelty_type, start_date, end_date, notes, created_at
    FROM core.user_novelties
    WHERE user_id = $1
      AND id <> $4
      AND start_date <= $3::date
      AND end_date   >= $2::date
    ORDER BY start_date ASC, end_date ASC, id ASC
    `,
    [userId, startISO, endISO, Number(excludeId)]
  );
  return rows;
}

export async function updateNovelty({ authUser, id, patch }) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const { novelty: current } = await ensureNoveltyInScope(client, authUser, id);

    const nextType =
      patch?.novelty_type != null ? String(patch.novelty_type).trim() : current.novelty_type;

    const nextStart =
      patch?.start_date != null ? toISODateOnly(patch.start_date) : toISODateOnly(current.start_date);
    const nextEnd =
      patch?.end_date != null ? toISODateOnly(patch.end_date) : toISODateOnly(current.end_date);

    const nextNotes = patch?.notes !== undefined ? (patch.notes ?? null) : current.notes;

    if (!nextStart || !nextEnd) {
      const err = new Error("start_date y end_date son requeridos (fecha válida).");
      err.status = 400;
      throw err;
    }

    if (new Date(`${nextEnd}T00:00:00Z`) < new Date(`${nextStart}T00:00:00Z`)) {
      const err = new Error("end_date no puede ser menor que start_date.");
      err.status = 400;
      throw err;
    }

    const overlaps = await findOverlapsExcludingId(client, current.user_id, nextStart, nextEnd, id);
    if (overlaps.length > 0) {
      const err = new Error("No se puede actualizar: el rango se solapa con una novedad existente.");
      err.status = 409;
      err.code = "NOVELTY_OVERLAP";
      err.overlaps = overlaps;
      throw err;
    }

    let updated;
    try {
      const { rows } = await client.query(
        `
        UPDATE core.user_novelties
        SET novelty_type = $2,
            start_date   = $3::date,
            end_date     = $4::date,
            notes        = $5,
            updated_at   = now()
        WHERE id = $1
        RETURNING id, user_id, novelty_type, start_date, end_date, notes, created_at, updated_at
        `,
        [id, nextType, nextStart, nextEnd, nextNotes]
      );
      updated = rows[0];
    } catch (pgErr) {
      if (pgErr?.code === "23505") {
        const err = new Error("Conflicto de unicidad: ya existe una novedad igual para ese usuario.");
        err.status = 409;
        err.code = "NOVELTY_DUPLICATE";
        throw err;
      }
      throw pgErr;
    }

    const prevStart = toISODateOnly(current.start_date);
    const prevEnd = toISODateOnly(current.end_date);

    const monthsPrev = monthsBetween(prevStart, prevEnd);
    const monthsNext = monthsBetween(nextStart, nextEnd);
    const months = unionMonths(monthsPrev, monthsNext);

    const monthly_updates = [];
    for (const m of months) {
      monthly_updates.push(await recalcUserMonthlyForPeriod(client, updated.user_id, m.year, m.month));
    }

    await client.query("COMMIT");

    const progress_recalc = await recalcProgressForMonths(months);

    return { ok: true, novelty: updated, monthly_updates, progress_recalc };
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

/** ===== DELETE ===== */
export async function deleteNovelty({ authUser, id }) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const { novelty: current } = await ensureNoveltyInScope(client, authUser, id);

    const startISO = toISODateOnly(current.start_date);
    const endISO = toISODateOnly(current.end_date);
    const months = monthsBetween(startISO, endISO);

    await client.query(`DELETE FROM core.user_novelties WHERE id=$1`, [id]);

    const monthly_updates = [];
    for (const m of months) {
      monthly_updates.push(await recalcUserMonthlyForPeriod(client, current.user_id, m.year, m.month));
    }

    await client.query("COMMIT");

    const progress_recalc = await recalcProgressForMonths(months);

    return { ok: true, deleted_id: id, monthly_updates, progress_recalc };
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

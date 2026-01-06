// services/budgets.service.js
import pool from "../config/database.js";
import { promoteSiappFromFullSales } from "./promote.siapp.service.js";

/* =========================================================
   Period helpers
========================================================= */

function parsePeriod(period) {
  const p = String(period || "").trim();
  const m = p.match(/^(\d{4})-(0[1-9]|1[0-2])$/);
  if (!m) return null;
  return { year: Number(m[1]), month: Number(m[2]), period: p };
}

function prevPeriod(period) {
  const per = parsePeriod(period);
  if (!per) return null;
  let y = per.year;
  let m = per.month - 1;
  if (m === 0) {
    m = 12;
    y -= 1;
  }
  return `${y}-${String(m).padStart(2, "0")}`;
}

function normalizeScope(scope) {
  // La columna existe en DB y el CHECK la fija a MONTHLY
  const s = String(scope || "MONTHLY").trim().toUpperCase();
  return s === "MONTHLY" ? "MONTHLY" : "MONTHLY";
}

function normalizeStatus(status) {
  const s = String(status || "ACTIVE").trim().toUpperCase();
  return ["DRAFT", "ACTIVE", "CLOSED"].includes(s) ? s : "ACTIVE";
}

function normalizeUnit(unit) {
  // DB: unit NOT NULL DEFAULT 'CONNECTIONS' + CHECK unit = 'CONNECTIONS'
  const u = String(unit || "CONNECTIONS").trim().toUpperCase();
  return u === "CONNECTIONS" ? "CONNECTIONS" : "CONNECTIONS";
}

function toNum(x, fallback = 0) {
  const n = Number(x);
  return Number.isFinite(n) ? n : fallback;
}

function toIntNonNeg(x, fallback = 0) {
  const n = Number(x);
  if (!Number.isFinite(n)) return fallback;
  const t = Math.trunc(n);
  return t >= 0 ? t : 0;
}

function normalizeBudgetRow(r) {
  return {
    id: Number(r.id),
    period: r.period,
    user_id: Number(r.user_id),
    scope: r.scope,
    budget_amount: Number(r.budget_amount || 0),
    unit: r.unit,
    status: r.status,
    updated_at: r.updated_at
  };
}


async function syncLegacyUserBudget(client, userId, budgetAmount) {
  const uid = Number(userId);
  if (!Number.isFinite(uid) || uid <= 0) return;

  const amount = toIntNonNeg(budgetAmount, 0);

  // Solo sincronizamos presupuesto legacy para asesores
  await client.query(
    `
    UPDATE core.users u
    SET
      presupuesto = $2::numeric,
      ejecutado = COALESCE(u.ejecutado, 0),
      cierre_porcentaje = COALESCE(u.cierre_porcentaje, 0),
      updated_at = now()
    WHERE u.id = $1
      AND u.role = 'ASESORIA'
    `,
    [uid, amount]
  );
}


/* =========================================================
   Monthly recalculation (dias/prorrateo) AUTORIZADO por budgets+novelties
   - Source of truth presupuesto_mes: core.budgets
========================================================= */

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

async function getBaseBudgetForUserPeriod(client, userId, periodStr, scopeStr = "MONTHLY") {
  const { rows } = await client.query(
    `
    SELECT b.budget_amount
    FROM core.budgets b
    WHERE b.user_id = $1
      AND b.period = $2
      AND b.scope = $3
      AND (b.status = 'ACTIVE' OR b.status IS NULL)
    ORDER BY b.updated_at DESC NULLS LAST, b.id DESC
    LIMIT 1
    `,
    [userId, periodStr, scopeStr]
  );

  if (!rows[0] || rows[0].budget_amount == null) return null;
  const n = Number(rows[0].budget_amount);
  return Number.isFinite(n) ? n : null;
}

async function recalcUserMonthlyForPeriod(client, userId, year, month, scopeStr = "MONTHLY") {
  const daysInMonth = await getDaysInMonth(client, year, month);
  const nonWorked = await countNonWorkedDaysInMonth(client, userId, year, month);
  const diasLaborados = Math.max(daysInMonth - nonWorked, 0);
  const periodStr = `${year}-${String(month).padStart(2, "0")}`;

  // Presupuesto base desde budgets. Si no existe, fallback a valor anterior o 13.
  const budgetBase = await getBaseBudgetForUserPeriod(client, userId, periodStr, scopeStr);

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
    user_id: Number(userId),
    days_in_month: daysInMonth,
    non_worked_days: nonWorked,
    dias_laborados: diasLaborados,
    presupuesto_mes: presupuestoMes,
    prorrateo
  };
}

/**
 * Recalcula progress (core.progress) para un periodo.
 * Nota: promote recalcula para todos los asesores con SIAPP+users, no solo los afectados.
 * Esto es lo que garantiza automatización sin botones.
 */
async function recalcProgressForPeriod(per) {
  const r = await promoteSiappFromFullSales({ period_year: per.year, period_month: per.month });
  return {
    period: per.period,
    upserted: r.upserted,
    matched_users: r.matched_users,
    total_sales_rows: r.total_sales_rows
  };
}

/* =========================================================
   READS
========================================================= */

export async function getBudgetsByCoordinator({ period, coordinator_id, scope = "MONTHLY" }) {
  const per = parsePeriod(period);
  if (!per) throw new Error("Falta o es inválido ?period=YYYY-MM");

  const coordId = Number(coordinator_id);
  if (!Number.isFinite(coordId) || coordId <= 0) throw new Error("coordinator_id inválido");

  const sc = normalizeScope(scope);

  // 1) Coordinador (obligatoriamente rol COORDINACION)
  const coordQ = await pool.query(
    `
    SELECT id, name, email, role, org_unit_id, active, document_id, district, district_claro, regional, cargo
    FROM core.users
    WHERE id = $1
      AND role = 'COORDINACION'
    LIMIT 1
    `,
    [coordId]
  );

  const coordinator = coordQ.rows[0];
  if (!coordinator) throw new Error("Coordinador no encontrado o rol inválido");

  // 2) Presupuesto del coordinador
  const coordBudgetQ = await pool.query(
    `
    SELECT id, period, user_id, scope, budget_amount, unit, status, updated_at
    FROM core.budgets
    WHERE period = $1 AND user_id = $2 AND scope = $3
    LIMIT 1
    `,
    [per.period, coordId, sc]
  );

  // 3) Asesores bajo el coordinador
  const advisorsQ = await pool.query(
    `
    SELECT
      u.id,
      u.name,
      u.document_id,
      u.email,
      u.active,
      u.district,
      u.district_claro,
      u.regional,
      u.cargo,
      b.id AS budget_id,
      b.budget_amount,
      b.unit,
      b.status,
      b.period,
      b.scope,
      b.updated_at
    FROM core.users u
    LEFT JOIN core.budgets b
      ON b.user_id = u.id
     AND b.period = $1
     AND b.scope = $2
    WHERE u.coordinator_id = $3
      AND u.role = 'ASESORIA'
    ORDER BY u.active DESC, u.name ASC
    `,
    [per.period, sc, coordId]
  );

  const advisors = advisorsQ.rows.map((r) => ({
    user_id: Number(r.id),
    name: r.name,
    document_id: r.document_id,
    email: r.email,
    active: r.active,
    district: r.district,
    district_claro: r.district_claro,
    regional: r.regional,
    cargo: r.cargo,
    budget: r.budget_id
      ? {
        id: Number(r.budget_id),
        period: r.period,
        scope: r.scope,
        budget_amount: Number(r.budget_amount || 0),
        unit: r.unit,
        status: r.status,
        updated_at: r.updated_at
      }
      : null
  }));

  const summary = {
    users_count: 1 + advisors.length,
    budget_total:
      Number(coordBudgetQ.rows[0]?.budget_amount || 0) +
      advisors.reduce((acc, a) => acc + Number(a.budget?.budget_amount || 0), 0),
    missing_count:
      (coordBudgetQ.rows[0] ? 0 : 1) + advisors.reduce((acc, a) => acc + (a.budget ? 0 : 1), 0)
  };

  return {
    period: per.period,
    scope: sc,
    coordinator: {
      id: Number(coordinator.id),
      name: coordinator.name,
      email: coordinator.email,
      role: coordinator.role,
      org_unit_id: coordinator.org_unit_id != null ? Number(coordinator.org_unit_id) : null,
      active: coordinator.active,
      document_id: coordinator.document_id,
      district: coordinator.district,
      district_claro: coordinator.district_claro,
      regional: coordinator.regional,
      cargo: coordinator.cargo,
      budget: coordBudgetQ.rows[0] ? normalizeBudgetRow(coordBudgetQ.rows[0]) : null
    },
    advisors,
    summary
  };
}

export async function getMissingBudgets({ period, scope = "MONTHLY", include_inactive = false }) {
  const per = parsePeriod(period);
  if (!per) throw new Error("Falta o es inválido ?period=YYYY-MM");
  const sc = normalizeScope(scope);

  const q = await pool.query(
    `
    SELECT
      u.id AS user_id,
      u.name,
      u.role,
      u.coordinator_id,
      u.org_unit_id,
      u.active
    FROM core.users u
    LEFT JOIN core.budgets b
      ON b.user_id = u.id
     AND b.period = $1
     AND b.scope = $2
    WHERE b.id IS NULL
      AND u.role IN ('COORDINACION','ASESORIA')
      AND ($3::bool = true OR u.active = true)
    ORDER BY u.role, u.name
    `,
    [per.period, sc, include_inactive]
  );

  return {
    period: per.period,
    scope: sc,
    total: q.rows.length,
    rows: q.rows.map((r) => ({
      user_id: Number(r.user_id),
      name: r.name,
      role: r.role,
      coordinator_id: r.coordinator_id != null ? Number(r.coordinator_id) : null,
      org_unit_id: r.org_unit_id != null ? Number(r.org_unit_id) : null,
      active: r.active
    }))
  };
}

/* =========================================================
   WRITES + AUTOMATION
   - Cada write:
     1) upsert budgets
     2) recalc core.user_monthly para usuarios afectados en ese periodo
     3) recalc core.progress para ese periodo (promote)
========================================================= */

/**
 * UPSERT masivo en UNA SOLA QUERY
 * items: [{ user_id, budget_amount | budget, status, unit }]
 */
export async function upsertBudgetsBatch({ period, scope = "MONTHLY", items = [], actor_user_id }) {
  const per = parsePeriod(period);
  if (!per) throw new Error("Falta o es inválido ?period=YYYY-MM");

  const sc = normalizeScope(scope);
  if (!Array.isArray(items) || items.length === 0) throw new Error("items[] requerido");

  const clean = items
    .map((it) => ({
      user_id: toNum(it.user_id, NaN),
      budget_amount: toIntNonNeg(it.budget_amount ?? it.budget ?? 0, 0),
      unit: normalizeUnit(it.unit),
      status: normalizeStatus(it.status)
    }))
    .filter((it) => Number.isFinite(it.user_id) && it.user_id > 0);

  if (clean.length === 0) throw new Error("items[] sin user_id válidos");

  const actor = actor_user_id != null ? Number(actor_user_id) : null;

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    if (actor_user_id != null) {
      await client.query(`SELECT set_config('app.actor_user_id', $1, true)`, [String(actor_user_id)]);
    }

    const q = await client.query(
      `
      WITH input AS (
        SELECT *
        FROM jsonb_to_recordset($3::jsonb)
          AS x(user_id int, budget_amount numeric, unit text, status text)
      )
      INSERT INTO core.budgets (period, user_id, scope, budget_amount, unit, status, created_by, updated_by)
      SELECT
        $1::text AS period,
        i.user_id,
        $2::text AS scope,
        GREATEST(TRUNC(i.budget_amount), 0)::numeric(14,0) AS budget_amount,
        'CONNECTIONS'::text AS unit,
        CASE
          WHEN upper(i.status) IN ('DRAFT','ACTIVE','CLOSED') THEN upper(i.status)
          ELSE 'ACTIVE'
        END AS status,
        $4::int AS created_by,
        $4::int AS updated_by
      FROM input i
      ON CONFLICT (period, user_id, scope)
      DO UPDATE SET
        budget_amount = EXCLUDED.budget_amount,
        unit          = EXCLUDED.unit,
        status        = EXCLUDED.status,
        updated_by    = EXCLUDED.updated_by,
        updated_at    = NOW()
      RETURNING id, period, user_id, scope, budget_amount, unit, status, updated_at
      `,
      [per.period, sc, JSON.stringify(clean), actor]
    );
    // Sync legacy (core.users.presupuesto) para asesores afectados
    for (const r of q.rows) {
      await syncLegacyUserBudget(client, Number(r.user_id), r.budget_amount);
    }

    // Recalcular user_monthly para usuarios afectados (MISMO TX)
    const affectedUserIds = Array.from(new Set(q.rows.map((r) => Number(r.user_id))));
    const monthly_updates = [];
    for (const uid of affectedUserIds) {
      monthly_updates.push(await recalcUserMonthlyForPeriod(client, uid, per.year, per.month, sc));
    }

    await client.query("COMMIT");

    // Recalcular progress (FUERA DE TX)
    const progress_recalc = await recalcProgressForPeriod(per);

    return {
      period: per.period,
      scope: sc,
      updated: q.rowCount,
      rows: q.rows.map((r) => normalizeBudgetRow(r)),
      monthly_updates,
      progress_recalc
    };
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

export async function updateBudgetById({ id, patch = {}, actor_user_id }) {
  const budgetId = Number(id);
  if (!Number.isFinite(budgetId) || budgetId <= 0) throw new Error("id inválido");

  const sets = [];
  const values = [];
  let i = 1;

  // Para automatización debemos saber period/user del budget
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    if (actor_user_id != null) {
      await client.query(`SELECT set_config('app.actor_user_id', $1, true)`, [String(actor_user_id)]);
    }

    const { rows: curRows } = await client.query(
      `
      SELECT id, period, user_id, scope
      FROM core.budgets
      WHERE id = $1
      LIMIT 1
      `,
      [budgetId]
    );

    const current = curRows[0];
    if (!current) throw new Error("Budget no encontrado");

    if (patch.budget_amount != null) {
      const amount = toIntNonNeg(patch.budget_amount, 0);
      sets.push(`budget_amount = $${i++}`);
      values.push(amount);
    }

    if (patch.unit != null) {
      sets.push(`unit = $${i++}`);
      values.push(normalizeUnit(patch.unit));
    }

    if (patch.status != null) {
      sets.push(`status = $${i++}`);
      values.push(normalizeStatus(patch.status));
    }

    if (sets.length === 0) throw new Error("No hay campos válidos para actualizar");

    sets.push(`updated_by = $${i++}`);
    values.push(actor_user_id != null ? Number(actor_user_id) : null);

    values.push(budgetId);

    const q = await client.query(
      `
      UPDATE core.budgets
      SET ${sets.join(", ")}, updated_at = NOW()
      WHERE id = $${i}
      RETURNING id, period, user_id, scope, budget_amount, unit, status, updated_at
      `,
      values
    );

    const updated = q.rows[0];

    if (!updated) throw new Error("Budget no encontrado");
    await syncLegacyUserBudget(client, Number(updated.user_id), updated.budget_amount);


    // Recalc user_monthly (mismo TX)
    const per = parsePeriod(updated.period);
    if (per) {
      await recalcUserMonthlyForPeriod(client, Number(updated.user_id), per.year, per.month, normalizeScope(updated.scope));
    }

    await client.query("COMMIT");

    // Progress fuera TX
    let progress_recalc = null;
    if (per) progress_recalc = await recalcProgressForPeriod(per);

    return {
      ok: true,
      data: normalizeBudgetRow(updated),
      monthly_updated: per
        ? { period: per.period, user_id: Number(updated.user_id) }
        : null,
      progress_recalc
    };
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

export async function copyBudgetsFromPreviousMonth({ period, scope = "MONTHLY", actor_user_id }) {
  const per = parsePeriod(period);
  if (!per) throw new Error("Falta o es inválido ?period=YYYY-MM");

  const from = prevPeriod(per.period);
  if (!from) throw new Error("No se pudo calcular mes anterior");

  const sc = normalizeScope(scope);
  const actor = actor_user_id != null ? Number(actor_user_id) : null;

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    if (actor_user_id != null) {
      await client.query(`SELECT set_config('app.actor_user_id', $1, true)`, [String(actor_user_id)]);
    }

    const q = await client.query(
      `
      INSERT INTO core.budgets (period, user_id, scope, budget_amount, unit, status, created_by, updated_by)
      SELECT
        $1 AS period,
        b.user_id,
        b.scope,
        b.budget_amount,
        COALESCE(b.unit, 'CONNECTIONS'::text) AS unit,
        'ACTIVE'::text AS status,
        $3::int AS created_by,
        $3::int AS updated_by
      FROM core.budgets b
      LEFT JOIN core.budgets dst
        ON dst.period = $1
       AND dst.user_id = b.user_id
       AND dst.scope = b.scope
      WHERE b.period = $2
        AND b.scope = $4
        AND dst.id IS NULL
      RETURNING id, period, user_id, scope, budget_amount, unit, status, updated_at
      `,
      [per.period, from, actor, sc]
    );
    for (const r of q.rows) {
      await syncLegacyUserBudget(client, Number(r.user_id), r.budget_amount);
    }

    // Recalc monthly para usuarios insertados (mismo TX)
    const affectedUserIds = Array.from(new Set(q.rows.map((r) => Number(r.user_id))));
    const monthly_updates = [];
    for (const uid of affectedUserIds) {
      monthly_updates.push(await recalcUserMonthlyForPeriod(client, uid, per.year, per.month, sc));
    }

    await client.query("COMMIT");

    // Progress fuera TX
    const progress_recalc = await recalcProgressForPeriod(per);

    return {
      from_period: from,
      to_period: per.period,
      scope: sc,
      inserted: q.rowCount,
      rows: q.rows.map((r) => normalizeBudgetRow(r)),
      monthly_updates,
      progress_recalc
    };
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

/**
 * Árbol jerárquico + agregados.
 * (No depende de currency; unit no es relevante para sumas)
 */
export async function getBudgetsTree({ period, scope = "MONTHLY" }) {
  const per = parsePeriod(period);
  if (!per) throw new Error("Falta o es inválido ?period=YYYY-MM");
  const sc = normalizeScope(scope);

  const unitsQ = await pool.query(
    `
    SELECT id, name, unit_type, parent_id
    FROM core.org_units
    WHERE unit_type IN ('GERENCIA','DIRECCION','COORDINACION')
    ORDER BY unit_type, name
    `
  );

  const units = unitsQ.rows;

  const managerUsersQ = await pool.query(
    `
    SELECT DISTINCT ON (org_unit_id)
      id, name, org_unit_id
    FROM core.users
    WHERE role = 'GERENCIA'
      AND org_unit_id IS NOT NULL
    ORDER BY org_unit_id, id
    `
  );

  const managerUserByUnit = new Map();
  for (const u of managerUsersQ.rows) managerUserByUnit.set(Number(u.org_unit_id), u);

  const directorUsersQ = await pool.query(
    `
    SELECT DISTINCT ON (org_unit_id)
      id, name, org_unit_id
    FROM core.users
    WHERE role = 'DIRECCION'
      AND org_unit_id IS NOT NULL
    ORDER BY org_unit_id, id
    `
  );

  const directorUserByUnit = new Map();
  for (const u of directorUsersQ.rows) directorUserByUnit.set(Number(u.org_unit_id), u);

  const coordUsersQ = await pool.query(
    `
    SELECT DISTINCT ON (org_unit_id)
      id, name, org_unit_id
    FROM core.users
    WHERE role = 'COORDINACION'
      AND org_unit_id IS NOT NULL
    ORDER BY org_unit_id, id
    `
  );

  const coordUserByUnit = new Map();
  for (const u of coordUsersQ.rows) coordUserByUnit.set(Number(u.org_unit_id), u);

  const aggQ = await pool.query(
    `
    WITH coord_user AS (
      SELECT u.id AS coordinator_user_id, u.org_unit_id AS coord_unit_id
      FROM core.users u
      WHERE u.role='COORDINACION' AND u.org_unit_id IS NOT NULL
    ),
    advisor_users AS (
      SELECT a.id AS advisor_user_id, cu.coord_unit_id
      FROM coord_user cu
      JOIN core.users a
        ON a.coordinator_id = cu.coordinator_user_id
       AND a.role = 'ASESORIA'
    ),
    all_users AS (
      SELECT coordinator_user_id AS user_id, coord_unit_id FROM coord_user
      UNION ALL
      SELECT advisor_user_id AS user_id, coord_unit_id FROM advisor_users
    )
    SELECT
      au.coord_unit_id,
      COUNT(*)::int AS users_count,
      SUM(COALESCE(b.budget_amount,0))::numeric AS budget_total,
      SUM(CASE WHEN b.id IS NULL THEN 1 ELSE 0 END)::int AS missing_count
    FROM all_users au
    LEFT JOIN core.budgets b
      ON b.user_id = au.user_id
     AND b.period = $1
     AND b.scope = $2
    GROUP BY au.coord_unit_id
    `,
    [per.period, sc]
  );

  const aggByCoordUnit = new Map();
  for (const r of aggQ.rows) aggByCoordUnit.set(Number(r.coord_unit_id), r);

  const byId = new Map(units.map((u) => [Number(u.id), { ...u, children: [] }]));
  const roots = [];

  for (const node of byId.values()) {
    if (node.parent_id) {
      const parent = byId.get(Number(node.parent_id));
      if (parent) parent.children.push(node);
    } else {
      roots.push(node);
    }
  }

  function enrich(node) {
    if (node.unit_type === "GERENCIA") {
      const managerUser = managerUserByUnit.get(Number(node.id)) || null;
      node.manager_user = managerUser
        ? { id: Number(managerUser.id), name: managerUser.name, org_unit_id: Number(managerUser.org_unit_id) }
        : null;
    }

    if (node.unit_type === "DIRECCION") {
      const directorUser = directorUserByUnit.get(Number(node.id)) || null;
      node.director_user = directorUser
        ? { id: Number(directorUser.id), name: directorUser.name, org_unit_id: Number(directorUser.org_unit_id) }
        : null;
    }

    if (node.unit_type === "COORDINACION") {
      const coordUser = coordUserByUnit.get(Number(node.id)) || null;
      const agg = aggByCoordUnit.get(Number(node.id)) || null;

      node.coordinator_user = coordUser
        ? { id: Number(coordUser.id), name: coordUser.name, org_unit_id: Number(coordUser.org_unit_id) }
        : null;

      node.budgets = agg
        ? {
          users_count: Number(agg.users_count || 0),
          budget_total: Number(agg.budget_total || 0),
          missing_count: Number(agg.missing_count || 0)
        }
        : { users_count: 0, budget_total: 0, missing_count: 0 };
    }

    for (const ch of node.children) enrich(ch);
    return node;
  }

  const tree = roots.map((r) => enrich(r));

  return {
    period: per.period,
    scope: sc,
    tree
  };
}

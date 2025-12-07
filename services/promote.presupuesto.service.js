import pool from '../config/database.js'
import { VALID_UNIT_TYPES } from './units.service.js'

function norm(s) {
  return String(s ?? '').trim().toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '')
}


async function findOrgUnitByName(client, name) {
  const q = `%${name}%`
  const { rows } = await client.query(
    `SELECT id, unit_type FROM core.org_units
     WHERE name ILIKE $1
     ORDER BY CASE
       WHEN unit_type='GERENCIA' THEN 1
       WHEN unit_type='DIRECCION' THEN 2
       WHEN unit_type='COORDINACION' THEN 3
       ELSE 4 END
     LIMIT 1`,
    [q]
  )
  return rows[0] || null
}

async function upsertGoal(client, period_year, period_month, org_unit_id, unit_type, product_line, target_amount) {
  const { rows } = await client.query(
    `INSERT INTO core.goals (period_year, period_month, product_line, target_amount, scope_level, scope_id)
     VALUES ($1,$2,$3,$4,'ORG_UNIT',$5)
     ON CONFLICT (period_year, period_month, scope_level, scope_id)
     DO UPDATE SET target_amount = EXCLUDED.target_amount
     RETURNING id`,
    [period_year, period_month, product_line || 'GENERAL', target_amount, org_unit_id]
  )
  return rows[0].id
}

async function createAssignmentsForChildren(client, parent_unit_id, goal_id, target_amount) {
  const { rows: children } = await client.query(
    `SELECT id FROM core.org_units WHERE parent_id = $1`,
    [parent_unit_id]
  )
  if (!children.length) return 0
  const perChild = Number(target_amount) / children.length
  let inserted = 0
  for (const c of children) {
    await client.query(
      `INSERT INTO core.assignments (goal_id, assigned_to_id, assigned_to_type, assigned_amount)
       VALUES ($1,$2,'ORG_UNIT',$3)
       ON CONFLICT (goal_id, assigned_to_id, assigned_to_type)
       DO UPDATE SET assigned_amount = EXCLUDED.assigned_amount`,
      [goal_id, c.id, perChild]
    )
    inserted++
  }
  return inserted
}

export async function promotePresupuestoFromStaging({ period_year, period_month }) {
  if (!period_year || !period_month) throw new Error('Falta periodo')
  const client = await pool.connect()
  try {
    await client.query('BEGIN')
    const { rows } = await client.query(`
      SELECT
        NULLIF(TRIM(periodo),'') AS periodo,
        NULLIF(TRIM(nivel),'')   AS nivel,
        NULLIF(TRIM(nombre),'')  AS nombre,
        presupuesto::numeric AS presupuesto
      FROM staging.presupuesto_jerarquia
      WHERE presupuesto IS NOT NULL
    `)

    let createdGoals = 0
    let updatedGoals = 0
    let createdAssignments = 0

    for (const r of rows) {
      const name = r.nombre
      const presupuesto = Number(r.presupuesto) || 0
      const nivel = r.nivel ? r.nivel.toUpperCase() : null

      const unit = await findOrgUnitByName(client, name)
      if (!unit) continue

      const goalId = await upsertGoal(
        client,
        period_year,
        period_month,
        unit.id,
        nivel || unit.unit_type,
        'GENERAL',
        presupuesto
      )

      createdGoals++
      createdAssignments += await createAssignmentsForChildren(client, unit.id, goalId, presupuesto)
    }

    await client.query('COMMIT')
    return { createdGoals, createdAssignments, totalRows: rows.length }
  } catch (e) {
    await client.query('ROLLBACK')
    throw e
  } finally {
    client.release()
  }
}

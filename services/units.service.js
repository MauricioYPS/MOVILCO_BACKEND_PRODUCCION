import pool from '../config/database.js'

export const VALID_UNIT_TYPES = ['GERENCIA', 'DIRECCION', 'COORDINACION', 'ASESORIA']
export const isValidUnitType = (t) => VALID_UNIT_TYPES.includes(String(t || '').toUpperCase())

export async function listUnits() {
  const { rows } = await pool.query(`
    SELECT id, name, unit_type, parent_id, created_at, updated_at
    FROM core.org_units
    ORDER BY id ASC
  `)
  return rows
}

export async function getUnitById(id) {
  const { rows } = await pool.query(`
    SELECT id, name, unit_type, parent_id, created_at, updated_at
    FROM core.org_units
    WHERE id = $1
  `, [id])
  return rows[0] || null
}

export async function parentExists(parent_id) {
  if (parent_id == null) return true
  const { rows } = await pool.query(`SELECT 1 FROM core.org_units WHERE id = $1`, [parent_id])
  return rows.length > 0
}

export async function wouldCreateCycle(id, newParentId) {
  if (newParentId == null) return false
  if (Number(id) === Number(newParentId)) return true
  const { rows } = await pool.query(`
    WITH RECURSIVE subtree AS (
      SELECT id, parent_id FROM core.org_units WHERE id = $1
      UNION ALL
      SELECT ou.id, ou.parent_id
      FROM core.org_units ou
      JOIN subtree s ON ou.parent_id = s.id
    )
    SELECT 1 FROM subtree WHERE id = $2
  `, [id, newParentId])
  return rows.length > 0
}

export async function hasUsers(unitId) {
  const { rows } = await pool.query(
    `SELECT 1 FROM core.users WHERE org_unit_id = $1 LIMIT 1`,
    [unitId]
  )
  return rows.length > 0
}

export async function createUnit({ name, unit_type, parent_id }) {
  const { rows } = await pool.query(`
    INSERT INTO core.org_units (name, unit_type, parent_id)
    VALUES ($1, $2, $3)
    RETURNING id, name, unit_type, parent_id, created_at, updated_at
  `, [name.trim(), String(unit_type).toUpperCase(), parent_id ?? null])
  return rows[0]
}

export async function updateUnit(id, { name, unit_type, parent_id }) {
  const client = await pool.connect()
  try {
    await client.query('BEGIN')
    const { rows } = await client.query(`
      UPDATE core.org_units
      SET name = $1,
          unit_type = $2,
          parent_id = $3,
          updated_at = now()
      WHERE id = $4
      RETURNING id, name, unit_type, parent_id, created_at, updated_at
    `, [name.trim(), String(unit_type).toUpperCase(), parent_id ?? null, id])
    await client.query('COMMIT')
    return rows[0]
  } catch (e) {
    await client.query('ROLLBACK')
    throw e
  } finally {
    client.release()
  }
}

export async function deleteUnit(id) {
  const client = await pool.connect()
  try {
    await client.query('BEGIN')
    await client.query(`DELETE FROM core.org_units WHERE id = $1`, [id])
    await client.query('COMMIT')
    return true
  } catch (e) {
    await client.query('ROLLBACK')
    throw e
  } finally {
    client.release()
  }
}

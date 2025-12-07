// services/promote.service.js
import pool from '../config/database.js'
import { VALID_UNIT_TYPES } from './units.service.js'

// Obtiene id si existe; si no, inserta y devuelve id
async function getOrCreateUnit(client, { name, unit_type, parent_id }) {
  const { rows: found } = await client.query(
    `SELECT id FROM core.org_units WHERE name = $1 AND unit_type = $2 AND
     (${parent_id == null ? 'parent_id IS NULL' : 'parent_id = $3'})`,
    parent_id == null ? [name, unit_type] : [name, unit_type, parent_id]
  )
  if (found[0]) return found[0].id

  const { rows: created } = await client.query(
    `INSERT INTO core.org_units (name, unit_type, parent_id)
     VALUES ($1,$2,$3)
     RETURNING id`,
    [name, unit_type, parent_id ?? null]
  )
  return created[0].id
}

export async function promoteEstructuraFromStaging() {
  const client = await pool.connect()
  try {
    await client.query('BEGIN')

    // Traer todas las filas de staging (normalizadas)
    const { rows } = await client.query(`
      SELECT
        NULLIF(TRIM(gerencia), '')  AS gerencia,
        NULLIF(TRIM(direccion), '') AS direccion,
        NULLIF(TRIM(coordinacion), '') AS coordinacion,
        NULLIF(TRIM(asesor), '') AS asesor,
        NULLIF(TRIM(distritos), '') AS distritos,
        NULLIF(TRIM(observaciones), '') AS observaciones
      FROM staging.estructura_jerarquia
    `)

    let created = 0, reused = 0
    for (const r of rows) {
      let gerId = null, dirId = null, coorId = null

      // 1) GERENCIA (raíz)
      if (r.gerencia) {
        gerId = await getOrCreateUnit(client, {
          name: r.gerencia,
          unit_type: 'GERENCIA',
          parent_id: null
        })
      }

      // 2) DIRECCION (hija de gerencia)
      if (r.direccion) {
        dirId = await getOrCreateUnit(client, {
          name: r.direccion,
          unit_type: 'DIRECCION',
          parent_id: gerId
        })
      }

      // 3) COORDINACION (hija de direccion)
      if (r.coordinacion) {
        coorId = await getOrCreateUnit(client, {
          name: r.coordinacion,
          unit_type: 'COORDINACION',
          parent_id: dirId
        })
      }

      // (Opcional) 4) ASESORIA como unidad o la dejamos para users
      // De momento NO creamos unidad "ASESORIA" ni user.
      // Más adelante, cuando importemos nómina, creamos usuarios y los colgamos de COORDINACION.

      // conteo aproximado (solo informativo)
      if (gerId || dirId || coorId) created++ // no exacto: puede contar reused; lo dejamos simple
    }

    await client.query('COMMIT')
    return { created, reused }
  } catch (e) {
    await client.query('ROLLBACK')
    throw e
  } finally {
    client.release()
  }
}

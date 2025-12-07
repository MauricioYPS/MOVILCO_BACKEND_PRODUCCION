import pool from '../config/database.js'

async function resolveUserId(client, cedula, nombre) {
  if (cedula) {
    const { rows } = await client.query(`SELECT id FROM core.users WHERE document_id = $1 LIMIT 1`, [String(cedula)])
    if (rows[0]) return rows[0].id
  }
  if (nombre) {
    const { rows } = await client.query(`SELECT id FROM core.users WHERE lower(name) = lower($1) LIMIT 1`, [String(nombre)])
    if (rows[0]) return rows[0].id
  }
  return null
}

export async function promoteNovedadesFromStaging() {
  const client = await pool.connect()
  try {
    await client.query('BEGIN')

    const { rows } = await client.query(`
      SELECT
        NULLIF(TRIM(tipo),'') AS tipo,
        NULLIF(TRIM(cedula),'') AS cedula,
        NULLIF(TRIM(nombre_funcionario),'') AS nombre,
        fecha_inicio,
        fecha_fin,
        notas
      FROM staging.novedades
      WHERE fecha_inicio IS NOT NULL AND fecha_fin IS NOT NULL
    `)

    let inserted = 0
    let skipped = 0

    for (const r of rows) {
      const userId = await resolveUserId(client, r.cedula, r.nombre)
      if (!userId) { skipped++; continue }

      if (new Date(r.fecha_fin) < new Date(r.fecha_inicio)) { skipped++; continue }

      await client.query(
        `INSERT INTO core.user_novelties (user_id, novelty_type, start_date, end_date, notes)
         VALUES ($1,$2,$3,$4,$5)`,
        [userId, r.tipo || 'NOVEDAD', r.fecha_inicio, r.fecha_fin, r.notas || null]
      )
      inserted++
    }

    await client.query('COMMIT')
    return { inserted, skipped, total: rows.length }
  } catch (e) {
    await client.query('ROLLBACK')
    throw e
  } finally {
    client.release()
  }
}

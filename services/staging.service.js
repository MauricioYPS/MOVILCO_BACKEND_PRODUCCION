import pool from '../config/database.js'

const ALLOWED = Object.freeze({
  estructura: 'staging.estructura_jerarquia',
  presupuesto: 'staging.presupuesto_jerarquia',
  nomina: 'staging.archivo_nomina',
  siapp: 'staging.siapp',
  novedades: 'staging.novedades'
})

export function resolveStagingTable(datasetKey) {
  const table = ALLOWED[datasetKey]
  if (!table) throw new Error('Tabla de staging no permitida')
  return table
}

export async function listStagingTable(tableName, { limit = 100, offset = 0 } = {}) {
  const safeLimit = Math.max(1, Math.min(Number(limit) || 100, 1000)) // tope 1000 por seguridad
  const safeOffset = Math.max(0, Number(offset) || 0)

  const client = await pool.connect()
  try {
    // ✅ 1) Verificar si la tabla tiene la columna raw_row
    const [schema, name] = tableName.includes('.') ? tableName.split('.') : ['staging', tableName]
    const checkSql = `
      SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2 AND column_name = 'raw_row'
      ) AS has_raw_row
    `
    const chk = await client.query(checkSql, [schema, name])
    const hasRaw = chk.rows[0]?.has_raw_row === true

    // ✅ 2) Calcular total de registros
    const totalResult = await client.query(`SELECT COUNT(*)::bigint AS total FROM ${tableName}`)
    const total = Number(totalResult.rows[0]?.total || 0)

    // ✅ 3) Query flexible: si no hay raw_row, generar fila numerada temporal
    const query = hasRaw
      ? `SELECT * FROM ${tableName} ORDER BY raw_row ASC LIMIT $1 OFFSET $2`
      : `
        SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT 1))::int AS raw_row
        FROM ${tableName}
        ORDER BY raw_row ASC
        LIMIT $1 OFFSET $2
      `

    const { rows } = await client.query(query, [safeLimit, safeOffset])

    return { total, rows, limit: safeLimit, offset: safeOffset }
  } finally {
    client.release()
  }
}

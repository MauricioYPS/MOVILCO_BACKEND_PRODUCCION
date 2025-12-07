// services/kpi.service.js
import pool from '../config/database.js'

/**
 * Parsea periodo YYYY-MM.
 */
export function parsePeriod(q) {
  if (!q) return null
  const m = String(q).match(/^(\d{4})-(\d{1,2})$/)
  if (!m) return null
  return { year: Number(m[1]), month: Number(m[2]) }
}

/**
 * KPI para una unidad organizacional.
 * Basado en core.progress (ya alimentado por SIAPP FULL).
 */
export async function kpiForUnit({ unit_id, year, month }) {
  const client = await pool.connect()
  try {
    // 1. Obtener unidad
    const { rows: metaRows } = await client.query(
      `SELECT id, name, unit_type, parent_id
       FROM core.org_units
       WHERE id = $1`,
      [unit_id]
    )
    if (metaRows.length === 0) return null
    const meta = metaRows[0]

    // 2. Obtener usuarios de la unidad
    const { rows: users } = await client.query(
      `
      SELECT u.id
      FROM core.users u
      WHERE u.org_unit_id = $1
      `,
      [unit_id]
    )

    if (users.length === 0) {
      return {
        unit_id: meta.id,
        unit_name: meta.name,
        unit_type: meta.unit_type,
        expected: 0,
        adjusted: 0,
        real_total: 0,
        compliance_global: 0,
        compliance_in: 0,
        met_global: false,
        met_in_district: false,
        users: []
      }
    }

    const userIds = users.map(u => u.id)

    // 3. Leer progress
    const { rows: progress } = await client.query(
      `
      SELECT
        user_id,
        real_total_count,
        real_in_count,
        expected_count,
        adjusted_count,
        compliance_global_percent,
        compliance_in_percent,
        met_global,
        met_in_district
      FROM core.progress
      WHERE period_year = $1
      AND period_month = $2
      AND user_id = ANY($3)
      `,
      [year, month, userIds]
    )

    let total_expected = 0
    let total_adjusted = 0
    let total_real = 0
    let sum_global = 0
    let sum_in = 0
    let met_global_count = 0
    let met_in_count = 0

    for (const p of progress) {
      total_expected += p.expected_count || 0
      total_adjusted += p.adjusted_count || 0
      total_real += p.real_total_count || 0
      sum_global += p.compliance_global_percent || 0
      sum_in += p.compliance_in_percent || 0

      if (p.met_global) met_global_count++
      if (p.met_in_district) met_in_count++
    }

    const countUsers = progress.length || 1

    return {
      unit_id: meta.id,
      unit_name: meta.name,
      unit_type: meta.unit_type,
      expected: total_expected,
      adjusted: total_adjusted,
      real_total: total_real,

      compliance_global: Number((sum_global / countUsers).toFixed(2)),
      compliance_in: Number((sum_in / countUsers).toFixed(2)),

      met_global: met_global_count === countUsers,
      met_in_district: met_in_count === countUsers,

      users: progress
    }

  } finally {
    client.release()
  }
}

/**
 * KPI para nivel (gerencia, dirección, coordinación).
 * Reuso la misma función recursivamente.
 */
export async function kpiForLevel({ level, year, month }) {
  const client = await pool.connect()
  try {
    const { rows: units } = await client.query(
      `
      SELECT id, name, unit_type
      FROM core.org_units
      WHERE unit_type = $1
      `,
      [level]
    )

    const out = []
    for (const u of units) {
      const r = await kpiForUnit({ unit_id: u.id, year, month })
      if (r) out.push(r)
    }
    return out
  } finally {
    client.release()
  }
}

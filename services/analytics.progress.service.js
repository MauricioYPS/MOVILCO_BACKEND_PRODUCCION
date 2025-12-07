// services/analytics.progress.service.js
import pool from '../config/database.js'

/**
 * Utilidad para interpretar periodos YYYY-MM.
 */
function parsePeriod(q) {
  const m = String(q || '').match(/^(\d{4})-(\d{1,2})$/)
  if (!m) return null
  return { year: Number(m[1]), month: Number(m[2]) }
}

/**
 * Construye una CTE dinámica de subusuarios según una unidad organizacional.
 * Mantengo la lógica original, ahora solo la perfecciono.
 */
function subunitsCTE(unit_id) {
  if (!unit_id) {
    return {
      cte: 'subusers AS (SELECT u.id FROM core.users u)',
      filter: ''
    }
  }

  return {
    cte: `
      subunits AS (
        SELECT id FROM core.org_units WHERE id = ${Number(unit_id)}
        UNION ALL
        SELECT u2.id
        FROM core.org_units u1
        JOIN core.org_units u2 ON u2.parent_id = u1.id
      ),
      subusers AS (
        SELECT u.id
        FROM core.users u
        JOIN subunits s ON s.id = u.org_unit_id
      )
    `,
    filter: 'JOIN subusers su ON su.id = p.user_id'
  }
}

/**
 * Dashboard principal basado en la vista enriquecida:
 * siapp.vw_progress_with_sales
 */
export async function analyticsProgress({ period, unit_id, limit = 50, offset = 0 }) {
  const client = await pool.connect()
  try {
    // 1) Parsear periodo YYYY-MM
    const per = parsePeriod(period)
    if (!per) {
      throw new Error('Periodo inválido. Usa formato YYYY-MM.')
    }

    const { year, month } = per

    // 2) Construir CTEs de unidades/subusuarios
    const { cte, filter } = subunitsCTE(unit_id)
    const safeLimit = Number(limit) || 50
    const safeOffset = Number(offset) || 0

    // 3) Query final
    const sql = `
      WITH
      ${cte},

      data AS (
        SELECT
          p.user_id,
          u.name AS usuario,
          u.document_id AS cedula,
          ou.name AS unidad,
          ou.unit_type AS tipo_unidad,

          -- progresos del mes
          p.real_in_count,
          p.real_out_count,
          p.real_total_count,
          p.expected_count,
          p.adjusted_count,
          p.compliance_in_percent,
          p.compliance_global_percent,
          p.met_in_district,
          p.met_global,

          -- ventas reales del SIAPP FULL
          sb.total_registros AS ventas_registros,
          sb.total_cantserv AS ventas_cantserv,
          sb.suma_tarifa_venta AS ventas_tarifa_total,
          sb.suma_comision_neta AS ventas_comision_total

        FROM core.progress p
        LEFT JOIN core.users u ON u.id = p.user_id
        LEFT JOIN core.org_units ou ON ou.id = u.org_unit_id

        LEFT JOIN (
          SELECT
            id_asesor,
            period_year,
            period_month,
            COUNT(*) AS total_registros,
            SUM(cantserv::numeric) AS total_cantserv,
            SUM(tarifa_venta::numeric) AS suma_tarifa_venta,
            SUM(comision_neta::numeric) AS suma_comision_neta
          FROM siapp.full_sales
          WHERE period_year = $1 AND period_month = $2
          GROUP BY id_asesor, period_year, period_month
        ) sb
        ON sb.id_asesor = u.document_id

        ${filter}

        WHERE p.period_year = $1 AND p.period_month = $2
      )

      SELECT *
      FROM data
      ORDER BY compliance_global_percent DESC
      LIMIT ${safeLimit}
      OFFSET ${safeOffset};
    `

    const { rows } = await client.query(sql, [year, month])

    return {
      ok: true,
      period_year: year,
      period_month: month,
      unit_id,
      limit: safeLimit,
      offset: safeOffset,
      count: rows.length,
      rows
    }

  } finally {
    client.release()
  }
}

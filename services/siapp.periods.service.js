// services/siapp.periods.service.js
import pool from "../config/database.js";

/**
 * Lista los periodos disponibles en siapp.full_sales.
 * Devuelve: [{ period_year, period_month, filas, period }]
 */
export async function listSiappPeriods() {
  const { rows } = await pool.query(`
    SELECT
      period_year::int,
      period_month::int,
      COUNT(*)::bigint AS filas
    FROM siapp.full_sales
    GROUP BY 1,2
    ORDER BY 1 DESC, 2 DESC
  `);

  return rows.map(r => ({
    period_year: Number(r.period_year),
    period_month: Number(r.period_month),
    filas: Number(r.filas),
    period: `${r.period_year}-${String(r.period_month).padStart(2, "0")}`
  }));
}

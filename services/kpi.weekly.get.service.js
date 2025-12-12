// ======================================================================
// KPI WEEKLY GET SERVICE — Versión Extendida limpia sin raw_json
// ======================================================================

import pool from "../config/database.js";

/**********************************************************************
 * Campos que SIEMPRE devolvemos (limpios, sin raw_json)
 **********************************************************************/
const BASE_SELECT = `
  id,
  week_start,
  week_end,
  year,
  week_number,
  asesor_id,
  documento,
  nombre,
  estado,
  org_unit_id,
  distrito_claro,
  dias_laborados,
  presupuesto_prorrateado,
  ventas_distrito,
  ventas_fuera,
  ventas_totales,
  cumple_distrito,
  cumple_global,
  created_at
`;

/**********************************************************************
 * 1) Semana específica (year + week)
 **********************************************************************/
export async function loadWeeklyKpi({ year, week_number }) {
  const q = `
    SELECT ${BASE_SELECT}
    FROM kpi.weekly_kpi_results
    WHERE year = $1
      AND week_number = $2
    ORDER BY asesor_id ASC
  `;
  const { rows } = await pool.query(q, [year, week_number]);
  return rows;
}

/**********************************************************************
 * 2) Todas las semanas del periodo YYYY-MM
 **********************************************************************/
export async function loadWeeklyKpiByPeriod(period) {
  const [y] = period.split("-").map(n => Number(n));

  const q = `
    SELECT ${BASE_SELECT}
    FROM kpi.weekly_kpi_results
    WHERE year = $1
    ORDER BY week_number ASC, asesor_id ASC
  `;
  const { rows } = await pool.query(q, [y]);
  return rows;
}

/**********************************************************************
 * 3) Filtrar por asesor + periodo
 **********************************************************************/
export async function loadWeeklyByAsesor(asesor_id, period) {
  const [y] = period.split("-").map(n => Number(n));

  const q = `
    SELECT ${BASE_SELECT}
    FROM kpi.weekly_kpi_results
    WHERE year = $1
      AND asesor_id = $2
    ORDER BY week_number ASC
  `;
  const { rows } = await pool.query(q, [y, asesor_id]);
  return rows;
}

/**********************************************************************
 * 4) Filtrar por coordinador + periodo
 *    Busca asesor_id → pertenece a un coordinador
 **********************************************************************/
export async function loadWeeklyByCoordinator(coord_id, period) {
  const [y] = period.split("-").map(n => Number(n));

  const q = `
    SELECT
      w.id,
      w.week_start,
      w.week_end,
      w.year,
      w.week_number,
      w.asesor_id,
      w.documento,
      w.nombre,
      w.estado,
      w.org_unit_id,
      w.distrito_claro,
      w.dias_laborados,
      w.presupuesto_prorrateado,
      w.ventas_distrito,
      w.ventas_fuera,
      w.ventas_totales,
      w.cumple_distrito,
      w.cumple_global,
      w.created_at
    FROM kpi.weekly_kpi_results w
    JOIN core.users u ON u.id = w.asesor_id
    WHERE w.year = $1
      AND u.coordinator_id = $2
    ORDER BY w.week_number ASC, w.asesor_id ASC
  `;

  const { rows } = await pool.query(q, [y, coord_id]);
  return rows;
}


/**********************************************************************
 * 5) Filtrar por distrito + periodo
 **********************************************************************/
export async function loadWeeklyByDistrict(distrito, period) {
  const [y] = period.split("-").map(n => Number(n));

  const q = `
    SELECT ${BASE_SELECT}
    FROM kpi.weekly_kpi_results
    WHERE year = $1
      AND distrito_claro ILIKE $2
    ORDER BY week_number ASC, asesor_id ASC
  `;
  const { rows } = await pool.query(q, [y, `%${distrito}%`]);
  return rows;
}

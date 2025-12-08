import pool from "../config/database.js";

/**
 * Crear o actualizar días laborados manualmente
 */
export async function setDiasLaboradosManual({ user_id, year, month, dias }) {
  const q = `
    INSERT INTO kpi.dias_laborados_manual (user_id, period_year, period_month, dias)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (user_id, period_year, period_month)
    DO UPDATE SET dias = EXCLUDED.dias, updated_at = NOW()
    RETURNING *
  `;

  const { rows } = await pool.query(q, [user_id, year, month, dias]);
  return rows[0];
}

/**
 * Obtener días laborados manuales (opcional)
 */
export async function getDiasLaboradosManual({ user_id, year, month }) {
  const q = `
    SELECT * 
    FROM kpi.dias_laborados_manual
    WHERE user_id = $1 AND period_year = $2 AND period_month = $3
  `;
  const { rows } = await pool.query(q, [user_id, year, month]);
  return rows[0] || null;
}

/**
 * Eliminar corrección manual (volver a cálculo automático)
 */
export async function deleteDiasLaboradosManual(id) {
  await pool.query(`DELETE FROM kpi.dias_laborados_manual WHERE id = $1`, [id]);
}

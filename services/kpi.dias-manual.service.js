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
 * Obtener días laborados manuales (1 user + 1 periodo)
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
 * LISTADO por periodo (para evitar N+1 requests en el front)
 * - include_user=false: devuelve solo registros manuales
 * - include_user=true: hace join con core.users para traer info del asesor
 */
export async function listDiasLaboradosManualByPeriod({ year, month, include_user = false }) {
  if (!include_user) {
    const q = `
      SELECT id, user_id, period_year, period_month, dias, updated_at
      FROM kpi.dias_laborados_manual
      WHERE period_year = $1 AND period_month = $2
      ORDER BY user_id ASC
    `;
    const { rows } = await pool.query(q, [year, month]);
    return rows;
  }

  // Con info del user para pintar UI sin consultas extra
  const q = `
    SELECT
      md.id,
      md.user_id,
      md.period_year,
      md.period_month,
      md.dias,
      md.updated_at,
      u.document_id,
      u.name,
      u.email,
      u.role,
      u.active,
      u.district,
      u.district_claro
    FROM kpi.dias_laborados_manual md
    JOIN core.users u ON u.id = md.user_id
    WHERE md.period_year = $1 AND md.period_month = $2
    ORDER BY u.name ASC NULLS LAST
  `;
  const { rows } = await pool.query(q, [year, month]);
  return rows;
}

/**
 * Eliminar corrección manual (volver a cálculo automático)
 */
export async function deleteDiasLaboradosManual(id) {
  await pool.query(`DELETE FROM kpi.dias_laborados_manual WHERE id = $1`, [id]);
}

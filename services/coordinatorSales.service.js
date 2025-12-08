import pool from "../config/database.js";

/**
 * Crear venta aprobada/corregida por el coordinador
 */
export async function createCoordinatorSale(data) {
  const keys = Object.keys(data);
  const values = Object.values(data);

  const columns = keys.join(",");
  const placeholders = keys.map((_, i) => `$${i + 1}`).join(",");

  const query = `
    INSERT INTO kpi.ventas_coordinador (${columns})
    VALUES (${placeholders})
    RETURNING *;
  `;

  const { rows } = await pool.query(query, values);
  return rows[0];
}

/**
 * Obtener ventas ya aprobadas por el coordinador
 */
export async function getCoordinatorSales({ coordinatorId, month }) {
  let params = [coordinatorId];
  let where = `WHERE coordinator_id = $1`;

  if (month) {
    params.push(month);
    where += ` AND TO_CHAR(fecha, 'YYYY-MM') = $2`;
  }

  const query = `
    SELECT *
    FROM kpi.ventas_coordinador
    ${where}
    ORDER BY fecha DESC;
  `;

  const { rows } = await pool.query(query, params);
  return rows;
}

/**
 * Actualizar una venta en validación del coordinador
 */
export async function updateCoordinatorSale(id, data) {
  const keys = Object.keys(data);
  const values = Object.values(data);

  const setClause = keys.map((k, i) => `${k} = $${i + 1}`).join(",");

  const query = `
    UPDATE kpi.ventas_coordinador
    SET ${setClause}, updated_at = NOW()
    WHERE id = $${keys.length + 1}
    RETURNING *;
  `;

  values.push(id);

  const { rows } = await pool.query(query, values);
  return rows[0];
}

/**
 * Marcar venta lista para exportación final al SIAPP
 */
export async function markSaleReadyForExport(id) {
  const query = `
    UPDATE kpi.ventas_coordinador
    SET ready_for_export = TRUE, updated_at = NOW()
    WHERE id = $1
    RETURNING *;
  `;
  const { rows } = await pool.query(query, [id]);
  return rows[0];
}

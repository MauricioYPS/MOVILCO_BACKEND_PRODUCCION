import pool from "../config/database.js";

/**
 * Crear registro final de venta manual (SIAPP generado)
 */
export async function createGeneratedSale(data) {
  const keys = Object.keys(data);
  const values = Object.values(data);

  const columns = keys.join(",");
  const placeholders = keys.map((_, i) => `$${i + 1}`).join(",");

  const query = `
    INSERT INTO siapp.generated_sales (${columns})
    VALUES (${placeholders})
    RETURNING *;
  `;

  const { rows } = await pool.query(query, values);
  return rows[0];
}

/**
 * Obtener ventas generadas por mes
 */
export async function getGeneratedSalesByMonth(month) {
  const query = `
    SELECT *
    FROM siapp.generated_sales
    WHERE TO_CHAR(fecha, 'YYYY-MM') = $1
    ORDER BY fecha DESC;
  `;

  const { rows } = await pool.query(query, [month]);
  return rows;
}

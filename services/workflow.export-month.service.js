// services/workflow.export-month.service.js
import pool from "../config/database.js";
import { insertIntoGeneratedSales } from "./workflow.service.js";

export async function getCoordinatorSalesPendingExport({ year, month, coordinatorId }) {
  const q = `
    SELECT *
    FROM kpi.ventas_coordinador
    WHERE ready_for_export = FALSE
      AND coordinator_id = $3
      AND EXTRACT(YEAR FROM fecha) = $1
      AND EXTRACT(MONTH FROM fecha) = $2
  `;

  const { rows } = await pool.query(q, [year, month, coordinatorId]);
  return rows;
}

export async function insertGeneratedSale(cleanSale) {
  return insertIntoGeneratedSales(cleanSale);
}

export async function markCoordinatorSaleExported(id) {
  const q = `
    UPDATE kpi.ventas_coordinador
    SET ready_for_export = TRUE
    WHERE id = $1
    RETURNING *;
  `;
  const { rows } = await pool.query(q, [id]);
  return rows[0];
}

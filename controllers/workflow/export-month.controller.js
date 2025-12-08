// controllers/workflow/export-month.controller.js
import pool from "../../config/database.js";
import {
  getCoordinatorSalesPendingExport,
  insertGeneratedSale,
  markCoordinatorSaleExported
} from "../../services/workflow.export-month.service.js";

export async function exportAllCoordinatorSales(req, res) {
  try {
    const { month, coordinator_id } = req.query;

    if (!month || !coordinator_id)
      return res.status(400).json({
        ok: false,
        error: "month=YYYY-MM y coordinator_id son obligatorios"
      });

    const match = month.match(/^(\d{4})-(\d{2})$/);
    if (!match)
      return res.status(400).json({
        ok: false,
        error: "Formato de mes inválido"
      });

    const year = Number(match[1]);
    const mon = Number(match[2]);

    const pending = await getCoordinatorSalesPendingExport({
      year,
      month: mon,
      coordinatorId: coordinator_id
    });

    if (pending.length === 0)
      return res.json({ ok: true, message: "No hay ventas para exportar", exported: 0 });

    const exported = [];
    const skipped = [];

    for (const sale of pending) {
      const dup = await pool.query(
        `SELECT id FROM siapp.generated_sales WHERE coordinator_sale_id = $1 LIMIT 1`,
        [sale.id]
      );

      if (dup.rows.length > 0) {
        skipped.push(sale.id);
        continue;
      }

      const { id, ready_for_export, created_at, updated_at, ...cleanSale } = sale;

      cleanSale.coordinator_sale_id = sale.id;

      await insertGeneratedSale(cleanSale);
      await markCoordinatorSaleExported(sale.id);

      exported.push(sale.id);
    }

    return res.json({
      ok: true,
      message: "Exportación masiva completada",
      total_exported: exported.length,
      total_skipped: skipped.length,
      exported_ids: exported,
      skipped_ids: skipped
    });

  } catch (error) {
    console.error("EXPORT MONTH ERROR:", error);
    return res.status(500).json({ ok: false, error: error.message });
  }
}

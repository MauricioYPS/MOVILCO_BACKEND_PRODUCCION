// controllers/workflow/workflow.controller.js
import pool from "../../config/database.js";
import {
  getRawSaleById,
  approveRawSale,
  insertCoordinatorSale,
  insertIntoGeneratedSales,
  markCoordinatorSaleExported
} from "../../services/workflow.service.js";

/* ============================================================
   APROBAR VENTA RAW
   ============================================================ */
export async function approveAndMoveToCoordinator(req, res) {
  try {
    const { id } = req.params;

    const rawSale = await getRawSaleById(id);
    if (!rawSale)
      return res.status(404).json({ ok: false, error: "Sale not found" });

    if (rawSale.estado_revision === "aprobado") {
      return res.status(400).json({
        ok: false,
        error: "Esta venta ya fue aprobada previamente"
      });
    }

    const exists = await pool.query(
      `SELECT id FROM kpi.ventas_coordinador WHERE raw_sale_id = $1 LIMIT 1`,
      [id]
    );

    if (exists.rows.length > 0) {
      return res.status(400).json({
        ok: false,
        error: "La venta ya fue movida anteriormente"
      });
    }

    await approveRawSale(id);

    const { id: _rid, created_at, updated_at, estado_revision, ...cleanSale } = rawSale;

    cleanSale.raw_sale_id = rawSale.id;

    const inserted = await insertCoordinatorSale(cleanSale);

    return res.json({ ok: true, data: inserted });

  } catch (error) {
    console.error("WORKFLOW ERROR:", error);
    return res.status(500).json({ ok: false, error: error.message });
  }
}

/* ============================================================
   EXPORTAR UNA SOLA VENTA
   ============================================================ */
export async function exportCoordinatorSale(req, res) {
  try {
    const { id } = req.params;

    const { rows } = await pool.query(
      `SELECT * FROM kpi.ventas_coordinador WHERE id = $1`,
      [id]
    );

    const sale = rows[0];
    if (!sale)
      return res.status(404).json({ ok: false, error: "Sale not found" });

    // evitar duplicados
    const dup = await pool.query(
      `SELECT id FROM siapp.generated_sales WHERE coordinator_sale_id = $1 LIMIT 1`,
      [sale.id]
    );

    if (dup.rows.length > 0) {
      return res.status(400).json({
        ok: false,
        error: "Esta venta ya está exportada"
      });
    }

    const { id: _sid, ready_for_export, created_at, updated_at, ...cleanSale } = sale;

    cleanSale.coordinator_sale_id = sale.id;

    const generated = await insertIntoGeneratedSales(cleanSale);

    await markCoordinatorSaleExported(sale.id);

    return res.json({ ok: true, data: generated });

  } catch (error) {
    console.error("EXPORT ERROR:", error);
    return res.status(500).json({ ok: false, error: error.message });
  }
}

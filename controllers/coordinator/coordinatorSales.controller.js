import {
  createCoordinatorSale,
  getCoordinatorSales,
  updateCoordinatorSale,
  markSaleReadyForExport
} from "../../services/coordinatorSales.service.js";

/**
 * Crear venta corregida/aprobada por el coordinador
 */
export async function postCoordinatorSale(req, res) {
  try {
    const data = req.body;
    const result = await createCoordinatorSale(data);

    return res.json({ ok: true, data: result });

  } catch (error) {
    console.error("ERROR CREATE COORDINATOR SALE:", error);
    return res.status(500).json({ ok: false, error: error.message });
  }
}

/**
 * Obtener ventas aprobadas por el coordinador
 */
export async function getCoordinatorSalesController(req, res) {
  try {
    const { coordinator_id, month } = req.query;

    const result = await getCoordinatorSales({
      coordinatorId: coordinator_id,
      month
    });

    return res.json({ ok: true, data: result });

  } catch (error) {
    console.error("ERROR GET COORDINATOR SALES:", error);
    return res.status(500).json({ ok: false, error: error.message });
  }
}

/**
 * Actualizar venta revisada por el coordinador
 */
export async function putCoordinatorSale(req, res) {
  try {
    const { id } = req.params;
    const payload = req.body;

    const result = await updateCoordinatorSale(id, payload);

    return res.json({ ok: true, data: result });

  } catch (error) {
    console.error("ERROR UPDATE COORDINATOR SALE:", error);
    return res.status(500).json({ ok: false, error: error.message });
  }
}

/**
 * Marcar venta como lista para exportación
 */
export async function markSaleExportController(req, res) {
  try {
    const { id } = req.params;

    const result = await markSaleReadyForExport(id);

    return res.json({ ok: true, data: result });

  } catch (error) {
    console.error("ERROR MARK SALE READY:", error);
    return res.status(500).json({ ok: false, error: error.message });
  }
}

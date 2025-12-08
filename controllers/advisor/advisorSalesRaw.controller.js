import {
  createAdvisorRawSale,
  getAdvisorRawSales,
  updateAdvisorRawSale,
  deleteAdvisorRawSale,
  getPendingAdvisorRawSales,
  setAdvisorRawSaleStatus
} from "../../services/advisorSalesRaw.service.js";

/**
 * Crear una venta registrada por el asesor
 */
export async function postAdvisorRawSale(req, res) {
  try {
    const data = req.body;
    const result = await createAdvisorRawSale(data);

    return res.json({ ok: true, data: result });

  } catch (error) {
    console.error("ERROR CREATE ADVISOR RAW SALE:", error);
    return res.status(500).json({ ok: false, error: error.message });
  }
}

/**
 * Obtener ventas del asesor (historial por mes)
 */
export async function getAdvisorRawSalesController(req, res) {
  try {
    const { advisor_id, month } = req.query;

    const result = await getAdvisorRawSales({
      advisorId: advisor_id,
      month
    });

    return res.json({ ok: true, data: result });

  } catch (error) {
    console.error("ERROR GET ADVISOR RAW SALES:", error);
    return res.status(500).json({ ok: false, error: error.message });
  }
}

/**
 * Actualizar venta cargada por el asesor
 */
export async function putAdvisorRawSale(req, res) {
  try {
    const { id } = req.params;
    const payload = req.body;

    const result = await updateAdvisorRawSale(id, payload);

    return res.json({ ok: true, data: result });

  } catch (error) {
    console.error("ERROR UPDATE ADVISOR RAW SALE:", error);
    return res.status(500).json({ ok: false, error: error.message });
  }
}

/**
 * Eliminar venta del asesor
 */
export async function deleteAdvisorRawSaleController(req, res) {
  try {
    const { id } = req.params;

    await deleteAdvisorRawSale(id);
    return res.json({ ok: true });

  } catch (error) {
    console.error("ERROR DELETE ADVISOR RAW SALE:", error);
    return res.status(500).json({ ok: false, error: error.message });
  }
}

/**
 * Ventas pendientes para el coordinador
 */
export async function getPendingAdvisorSales(req, res) {
  try {
    const { coordinator_id } = req.query;

    const result = await getPendingAdvisorRawSales(coordinator_id);

    return res.json({ ok: true, data: result });

  } catch (error) {
    console.error("ERROR GET PENDING ADVISOR SALES:", error);
    return res.status(500).json({ ok: false, error: error.message });
  }
}

/**
 * Cambiar estado de revisión: pendiente → aprobado/rechazado
 */
export async function setAdvisorRawSaleStatusController(req, res) {
  try {
    const { id } = req.params;
    const { status } = req.body;

    const result = await setAdvisorRawSaleStatus(id, status);

    return res.json({ ok: true, data: result });

  } catch (error) {
    console.error("ERROR UPDATE RAW SALE STATUS:", error);
    return res.status(500).json({ ok: false, error: error.message });
  }
}

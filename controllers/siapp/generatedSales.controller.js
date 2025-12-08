import {
  createGeneratedSale,
  getGeneratedSalesByMonth
} from "../../services/generatedSales.service.js";

/**
 * Insertar venta final (aprobada) al SIAPP generado
 */
export async function postGeneratedSale(req, res) {
  try {
    const data = req.body;

    const result = await createGeneratedSale(data);

    return res.json({ ok: true, data: result });

  } catch (error) {
    console.error("ERROR CREATE GENERATED SALE:", error);
    return res.status(500).json({ ok: false, error: error.message });
  }
}

/**
 * Obtener ventas del SIAPP generado por mes
 */
export async function getGeneratedSalesController(req, res) {
  try {
    const { month } = req.query;

    const result = await getGeneratedSalesByMonth(month);

    return res.json({ ok: true, data: result });

  } catch (error) {
    console.error("ERROR GET GENERATED SALES:", error);
    return res.status(500).json({ ok: false, error: error.message });
  }
}

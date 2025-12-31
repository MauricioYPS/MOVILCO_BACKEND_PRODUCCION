// controllers/siapp/monthly.sales.controller.js
import { getMonthlySalesDetail } from "../../services/siapp.monthly-sales.service.js";

export async function getMonthlySales(req, res) {
  try {
    const {
      period,
      limit,
      offset,
      q,
      advisor_id,
      only_in,
      district_mode
    } = req.query;

    const result = await getMonthlySalesDetail({
      period,
      limit,
      offset,
      q,
      advisor_id,
      only_in,
      district_mode: (district_mode ? String(district_mode).trim() : "auto")
    });

    // El service ya retorna ok:true; no lo dupliquemos
    return res.json(result);
  } catch (error) {
    console.error("[MONTHLY SALES ERROR]", error);

    // Si es error de validación de input (period, etc.), lo tratamos como 400
    const msg = String(error?.message || "");
    const isBadRequest =
      msg.includes("Falta o es inválido ?period=YYYY-MM") ||
      msg.includes("Formato") ||
      msg.includes("inválido") ||
      msg.includes("invalid") ||
      msg.includes("Use YYYY-MM");

    return res.status(isBadRequest ? 400 : 500).json({
      ok: false,
      error: msg || "Error al consultar ventas del mes"
    });
  }
}

// controllers/siapp/monthly.sales.summary.controller.js
import { getMonthlySalesSummary } from "../../services/siapp.monthly-sales.summary.service.js";

export async function getMonthlySalesSummaryController(req, res) {
  try {
    const { period, advisor_id, district_mode } = req.query;

    const result = await getMonthlySalesSummary({
      period,
      advisor_id,
      district_mode: district_mode || "auto"
    });

    return res.json({ ok: true, ...result });
  } catch (error) {
    console.error("[MONTHLY SALES SUMMARY ERROR]", error);
    return res.status(400).json({
      ok: false,
      error: error.message || "Error al consultar resumen de ventas del mes"
    });
  }
}

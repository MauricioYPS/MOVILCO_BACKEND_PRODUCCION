// controllers/siapp/monthly.progress.details.controller.js
import { getMonthlyProgressDetails } from "../../services/siapp.monthly-progress.details.service.js";

export async function getMonthlyProgressDetailsController(req, res) {
  try {
    const { period, advisor_id, district_mode } = req.query;

    const result = await getMonthlyProgressDetails({
      period,
      advisor_id,
      district_mode: district_mode || "auto"
    });

    return res.json({ ok: true, ...result });
  } catch (error) {
    console.error("[MONTHLY PROGRESS DETAILS ERROR]", error);
    return res.status(400).json({
      ok: false,
      error: error?.message || "Error al consultar detalles de progress del mes"
    });
  }
}

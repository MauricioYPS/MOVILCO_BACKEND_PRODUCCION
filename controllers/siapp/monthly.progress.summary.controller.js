// controllers/siapp/monthly.progress.summary.controller.js
import { getMonthlyProgressSummary } from "../../services/siapp.monthly-progress.summary.service.js";

export async function getMonthlyProgressSummaryController(req, res) {
  try {
    const {
      period,
      limit,
      offset,
      q,
      only_met_in,
      only_met_global,
      only_contracted,
      only_in_payroll
    } = req.query;

    const result = await getMonthlyProgressSummary({
      period,
      limit,
      offset,
      q,
      only_met_in,
      only_met_global,
      only_contracted,
      only_in_payroll
    });

    return res.json({ ok: true, ...result });
  } catch (error) {
    console.error("[MONTHLY PROGRESS SUMMARY ERROR]", error);
    return res.status(400).json({
      ok: false,
      error: error.message || "Error al consultar resumen de progress del mes"
    });
  }
}

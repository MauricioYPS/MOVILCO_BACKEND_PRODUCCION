// controllers/siapp/monthly.close.controller.js
import { closeMonthlyPeriod } from "../../services/siapp.monthly-close.service.js";

export async function closeMonthly(req, res) {
  try {
    const { period } = req.query;

    const result = await closeMonthlyPeriod({ period });

    return res.json({
      ok: true,
      message: `Cierre mensual ejecutado correctamente para ${result.period}`,
      ...result
    });
  } catch (error) {
    console.error("[MONTHLY CLOSE ERROR]", error);
    return res.status(400).json({
      ok: false,
      error: error.message || "Error al ejecutar cierre mensual"
    });
  }
}

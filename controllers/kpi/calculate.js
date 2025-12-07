// ======================================================================
// KPI CALCULATE CONTROLLER — Versión final corregida 2025-12
// ======================================================================

import { calculateKpiForPeriod } from "../../services/kpi.calculate.service.js";

export async function kpiCalculateController(req, res) {
  try {
    const period = req.query.period || req.query.periodo;

    if (!period) {
      return res.status(400).json({
        ok: false,
        message: "Debes enviar ?period=YYYY-MM"
      });
    }

    console.log("📊 Ejecutando cálculo KPI para periodo:", period);

    const result = await calculateKpiForPeriod(period);

    return res.status(200).json({
      ok: true,
      message: "KPI calculado correctamente",
      ...result
    });

  } catch (error) {
    console.error("❌ Error en kpiCalculateController:", error);

    return res.status(500).json({
      ok: false,
      message: "Error interno al calcular KPI",
      error: error.message
    });
  }
}

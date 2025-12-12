/**************************************************************
 * KPI SAVE CONTROLLER — Versión FINAL
 **************************************************************/

import { saveKpiForPeriod } from "../../services/kpi.save.service.js";

export async function saveKpiController(req, res) {
  try {
    const { period } = req.query;

    if (!period) {
      return res.status(400).json({
        ok: false,
        error: "Falta parámetro ?period=YYYY-MM"
      });
    }

    const match = String(period).match(/^(\d{4})-(\d{2})$/);
    if (!match) {
      return res.status(400).json({
        ok: false,
        error: "Formato inválido. Usa ?period=YYYY-MM"
      });
    }

    console.log("\n====================================");
    console.log(`[KPI SAVE] Solicitud recibida para periodo ${period}`);
    console.log("====================================\n");

    const result = await saveKpiForPeriod(period);

    console.log("\n====================================");
    console.log(`[KPI SAVE] Proceso completado para ${period}`);
    console.log(`Registros insertados: ${result.saved}`);
    console.log("====================================\n");

    return res.json({
      ok: true,
      message: result.message,
      period: result.period,

      registros_guardados: result.saved,
      registros_omitidos: result.omitted,

      total_ventas_reales: result.total_ventas_reales,
      total_ventas_registradas: result.total_ventas_registradas,
      total_ventas_desconocidas: result.total_ventas_desconocidas
    });

  } catch (e) {
    console.error("\n🔥 [KPI SAVE Controller] ERROR:");
    console.error(e);
    console.error("====================================\n");

    return res.status(500).json({
      ok: false,
      error: "No se pudo guardar los KPI del periodo",
      detail: e.message
    });
  }
}

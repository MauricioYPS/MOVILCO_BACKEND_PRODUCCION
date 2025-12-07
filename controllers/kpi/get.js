// controllers/kpi/get.js
import { getKpiForPeriod } from "../../services/kpi.get.service.js";

export async function getKpiController(req, res) {
  try {
    const { period, ...filters } = req.query;

    if (!period) {
      return res.status(400).json({ ok: false, error: "Falta parámetro ?period=YYYY-MM" });
    }

    console.log("\n========================================");
    console.log("[KPI GET] Nueva solicitud");
    console.log(`Periodo archivo: ${period}`);
    console.log("Filtros:", filters);
    console.log("========================================\n");

    const result = await getKpiForPeriod(period, filters);

    return res.json({ ok: true, ...result });

  } catch (e) {
    console.error("\n [KPI GET Controller] Error detectado:");
    console.error(e);
    console.error("========================================\n");

    return res.status(500).json({
      ok: false,
      error: "No se pudo obtener KPI",
      detail: e.message
    });
  }
}

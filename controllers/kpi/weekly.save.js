import { saveWeeklyKpi } from "../../services/kpi.weekly.save.service.js";

export async function weeklyKpiSaveController(req, res) {
  try {
    const { period } = req.query;

    if (!period || !period.match(/^\d{4}-\d{2}$/)) {
      return res.status(400).json({
        ok: false,
        error: "Debes enviar ?period=YYYY-MM"
      });
    }

    console.log(`\n📦 Guardando KPI semanal para ${period}...\n`);

    const result = await saveWeeklyKpi(period);

    return res.json({
      ok: true,
      ...result
    });

  } catch (e) {
    console.error("[WEEKLY SAVE CONTROLLER ERROR]", e);

    return res.status(500).json({
      ok: false,
      error: "Error interno al guardar KPI semanal",
      detail: e.message
    });
  }
}

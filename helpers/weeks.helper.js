import { loadWeeklyKpi } from "../../services/kpi.weekly.get.service.js";

export async function weeklyKpiGetController(req, res) {
  try {
    const { year, week } = req.query;

    if (!year || !week) {
      return res.status(400).json({
        ok: false,
        error: "Debes enviar ?year=YYYY&week=NN"
      });
    }

    const rows = await loadWeeklyKpi({ year, week });
    
    return res.status(200).json({
      ok: true,
      year,
      week,
      total: rows.length,
      data: rows
    });

  } catch (err) {
    console.error("[KPI WEEKLY GET ERROR]", err);
    return res.status(500).json({
      ok: false,
      error: "No se pudo cargar KPI semanal",
      detail: err.message
    });
  }
}

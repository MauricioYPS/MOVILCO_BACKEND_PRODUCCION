import { calculateWeeklyKpi } from "../../services/kpi.weekly.calculate.service.js";

export async function weeklyKpiCalculateController(req, res) {
  try {
    const period = req.query.period;
    if (!period) {
      return res.status(400).json({ ok: false, error: "Falta ?period=YYYY-MM" });
    }

    const result = await calculateWeeklyKpi(period);
    return res.json(result);

  } catch (e) {
    console.error("[KPI WEEKLY CALCULATE ERROR]", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
}

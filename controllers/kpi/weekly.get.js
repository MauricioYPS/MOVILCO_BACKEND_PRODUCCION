// ======================================================================
// KPI WEEKLY GET CONTROLLER — Versión Final limpia
// ======================================================================

import {
  loadWeeklyKpi,
  loadWeeklyKpiByPeriod,
  loadWeeklyByAsesor,
  loadWeeklyByCoordinator,
  loadWeeklyByDistrict
} from "../../services/kpi.weekly.get.service.js";

export async function weeklyKpiGetController(req, res) {
  try {
    const { year, week, period, asesor_id, coord_id, distrito } = req.query;

    // 1) Filtrar por asesor
    if (asesor_id && period) {
      const rows = await loadWeeklyByAsesor(Number(asesor_id), period);
      return res.json({ ok: true, rows });
    }

    // 2) Filtrar por coordinador
    if (coord_id && period) {
      const rows = await loadWeeklyByCoordinator(Number(coord_id), period);
      return res.json({ ok: true, rows });
    }

    // 3) Filtrar por distrito
    if (distrito && period) {
      const rows = await loadWeeklyByDistrict(distrito, period);
      return res.json({ ok: true, rows });
    }

    // 4) Semana puntual
    if (year && week) {
      const rows = await loadWeeklyKpi({
        year: Number(year),
        week_number: Number(week)
      });
      return res.json({ ok: true, rows });
    }

    // 5) Todas las semanas del periodo
    if (period) {
      const rows = await loadWeeklyKpiByPeriod(period);
      return res.json({ ok: true, rows });
    }

    return res.status(400).json({
      ok: false,
      error:
        "Debes enviar uno de los siguientes filtros: ?asesor_id=, ?coord_id=, ?distrito=, ?year=YYYY&week=N, o ?period=YYYY-MM"
    });

  } catch (e) {
    console.error("[KPI WEEKLY GET ERROR]", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
}

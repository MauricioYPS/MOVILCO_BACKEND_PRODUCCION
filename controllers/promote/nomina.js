// controllers/promote/nomina.js
import { promoteNominaFromStaging } from "../../services/promote.nomina.service.js";

// Acepta el periodo por:
// - query:  ?period=YYYY-MM  (ej. 2025-10)
// - o por:  ?year=2025&month=10
export async function promoteNomina(req, res) {
  try {
    let year, month;

    if (req.query.period) {
      const m = String(req.query.period).match(/^(\d{4})-(\d{1,2})$/);
      if (m) {
        year = Number(m[1]);
        month = Number(m[2]);
      }
    }

    if (!year || !month) {
      if (req.query.year && req.query.month) {
        year = Number(req.query.year);
        month = Number(req.query.month);
      }
    }

    if (!year || !month || month < 1 || month > 12) {
      return res
        .status(400)
        .json({ ok: false, error: "Provee el periodo: ?period=YYYY-MM o ?year=YYYY&month=MM" });
    }

    const result = await promoteNominaFromStaging({ period_year: year, period_month: month });

    return res.json({
      ok: true,
      period_year: year,
      period_month: month,
      ...result
    });
  } catch (e) {
    console.error("[PROMOTE nomina]", e);
    return res.status(500).json({
      ok: false,
      error: "No se pudo promover la nómina",
      detail: e.message
    });
  }
}

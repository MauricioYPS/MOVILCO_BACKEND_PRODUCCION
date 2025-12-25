// controllers/siapp/monthlyNomina.js
import { getMonthlyNominaPreview } from "../../services/siapp.monthly-nomina.service.js";

export async function monthlyNominaPreview(req, res) {
  try {
    const { period, q = null, limit = 200, offset = 0 } = req.query;

    const result = await getMonthlyNominaPreview({
      period,
      q,
      limit,
      offset
    });

    return res.json({ ok: true, ...result });
  } catch (e) {
    console.error("[SIAPP MONTHLY NOMINA]", e);
    return res.status(400).json({
      ok: false,
      error: e.message || "Error generando preview nómina"
    });
  }
}

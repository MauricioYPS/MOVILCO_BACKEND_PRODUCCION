// controllers/siapp/periods.js
import { listSiappPeriods } from "../../services/siapp.periods.service.js";

export async function getSiappPeriods(req, res) {
  try {
    const periods = await listSiappPeriods();
    return res.json({ ok: true, total: periods.length, periods });
  } catch (e) {
    console.error("[SIAPP PERIODS]", e);
    return res.status(500).json({ ok: false, error: "No se pudieron listar los periodos", detail: e.message });
  }
}
 
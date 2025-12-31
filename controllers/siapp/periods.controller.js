// controllers/siapp/periods.js
import { listSiappPeriods } from "../../services/siapp.periods.service.js";

export async function getSiappPeriods(req, res) {
  try {
    const { year = null, from = null, to = null } = req.query;

    const periods = await listSiappPeriods({ year, from, to });

    return res.json({
      ok: true,
      total: periods.length,
      periods
    });
  } catch (e) {
    console.error("[SIAPP PERIODS]", e);

    const status = Number(e?.status) || 500;

    return res.status(status).json({
      ok: false,
      error:
        status === 400
          ? (e.message || "Parámetros inválidos")
          : "No se pudieron listar los periodos",
      detail: e.message
    });
  }
}

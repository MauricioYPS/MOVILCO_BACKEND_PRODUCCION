// controllers/siapp/monthlyAdvisors.js
// import { listMonthlyAdvisors } from "../../services/siapp.monthly-advisors.service.js";

export async function getMonthlyAdvisors(req, res) {
  try {
    const { period, q = null, limit = 200, offset = 0, order = "ventas_desc" } = req.query;

    const result = await listMonthlyAdvisors({
      period,
      q,
      limit,
      offset,
      order
    });

    return res.json({ ok: true, ...result });
  } catch (e) {
    console.error("[SIAPP MONTHLY ADVISORS]", e);
    return res.status(400).json({
      ok: false,
      error: e.message || "Error al listar asesores del mes"
    });
  }
}

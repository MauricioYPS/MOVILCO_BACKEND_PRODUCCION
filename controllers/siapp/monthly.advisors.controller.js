// controllers/siapp/monthly.advisors.controller.js
import { listMonthlyAdvisors } from "../../services/siapp.monthly-advisors.service.js";

export async function getMonthlyAdvisorsController(req, res) {
  try {
    const {
      period,
      q = null,
      limit = 200,
      offset = 0,
      order = "ventas_desc",
      only_with_user = null
    } = req.query;

    const result = await listMonthlyAdvisors({
      period,
      q,
      limit,
      offset,
      order,
      only_with_user
    });

    return res.json({ ok: true, ...result });
  } catch (error) {
    console.error("[MONTHLY ADVISORS ERROR]", error);

    const status = Number(error?.status) || 400;

    return res.status(status).json({
      ok: false,
      error: error?.message || "Error al consultar asesores del mes"
    });
  }
}

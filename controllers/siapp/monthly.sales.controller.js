// controllers/siapp/monthly.sales.controller.js
import { getMonthlySalesDetail } from "../../services/siapp.monthly-sales.service.js";

export async function getMonthlySales(req, res) {
  try {
    const { period, limit, offset, q, advisor_id, only_in, district_mode } = req.query;

    const result = await getMonthlySalesDetail({
      period,
      limit,
      offset,
      q,
      advisor_id,
      only_in,
      district_mode: district_mode || "auto"
    });

    return res.json({ ok: true, ...result });
  } catch (error) {
    console.error("[MONTHLY SALES ERROR]", error);
    return res.status(400).json({
      ok: false,
      error: error.message || "Error al consultar ventas del mes"
    });
  }
}

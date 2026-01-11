// controllers/promote/presupuesto.js
import { promotePresupuestoFromStaging } from "../../services/promote.presupuesto.service.js";

export async function promotePresupuesto(req, res) {
  try {
    const { period, batch_id } = req.body || {};

    if (!period) {
      return res.status(400).json({
        ok: false,
        error: "Falta period en body (ej: { period: '2026-01', batch_id: '...' })",
      });
    }

    const actor_user_id = req.user?.id ?? null;

    const result = await promotePresupuestoFromStaging({
      period,
      batch_id: batch_id || null,
      actor_user_id,
    });

    return res.json({ ok: true, ...result });
  } catch (e) {
    console.error("[PROMOTE presupuesto]", e);
    return res.status(500).json({
      ok: false,
      error: "No se pudo promover presupuesto",
      detail: e.message,
    });
  }
}

import { promoteNovedadesFromStaging } from "../../services/promote.novedades.service.js";

export async function promoteNovedadesController(req, res) {
  try {
    const { batch_id } = req.body || {};
    const actor_user_id = req.user?.id ?? null;

    const result = await promoteNovedadesFromStaging({
      batch_id: batch_id || null,
      actor_user_id
    });

    return res.json({ ok: true, ...result });
  } catch (e) {
    console.error("[PROMOTE NOVEDADES]", e);
    return res.status(400).json({
      ok: false,
      error: e?.message || "No se pudieron promover las novedades"
    });
  }
}

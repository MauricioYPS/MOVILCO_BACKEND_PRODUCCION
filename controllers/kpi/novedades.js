import { createNovedad, getNovedadesFiltered, deleteNovedad } from "../../services/kpi.novedades.service.js";

export async function postNovedadController(req, res) {
  try {
    const { user_id, tipo, fecha_inicio, fecha_fin, descripcion } = req.body;

    if (!user_id || !tipo || !fecha_inicio || !fecha_fin) {
      return res.status(400).json({ ok: false, error: "Faltan campos obligatorios" });
    }

    const novedad = await createNovedad({
      user_id,
      tipo,
      fecha_inicio,
      fecha_fin,
      descripcion
    });

    res.json({ ok: true, novedad });

  } catch (e) {
    console.error("ERROR POST NOVEDAD:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
}

export async function getNovedadesController(req, res) {
  try {
    const { user_id, month } = req.query;

    const data = await getNovedadesFiltered({ user_id, month });

    return res.json({ ok: true, data });

  } catch (e) {
    console.error("ERROR GET NOVEDADES:", e);
    return res.status(500).json({
      ok: false,
      error: e.message
    });
  }
}

export async function deleteNovedadController(req, res) {
  try {
    const { id } = req.params;

    await deleteNovedad(id);
    res.json({ ok: true });

  } catch (e) {
    console.error("ERROR DELETE NOVEDAD:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
}

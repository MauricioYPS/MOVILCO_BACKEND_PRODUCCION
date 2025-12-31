import {
  listNovelties,
  listRecentNovelties,
  getNoveltyById,
  updateNovelty,
  deleteNovelty
} from "../../services/novedades.crud.service.js";

function toInt(v, def) {
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
}

export async function listNoveltiesController(req, res) {
  try {
    const { date_from = null, date_to = null, q = null } = req.query;
    const limit = Math.min(toInt(req.query.limit, 50), 200);
    const offset = Math.max(toInt(req.query.offset, 0), 0);

    const result = await listNovelties({
      authUser: req.user,
      date_from,
      date_to,
      q,
      limit,
      offset
    });

    return res.json(result);
  } catch (e) {
    console.error("[LIST NOVELTIES]", e);
    return res.status(Number(e?.status) || 500).json({
      ok: false,
      error: e?.message || "No se pudo listar novedades"
    });
  }
}

export async function recentNoveltiesController(req, res) {
  try {
    const days = Math.min(toInt(req.query.days, 3), 30);
    const limit = Math.min(toInt(req.query.limit, 50), 200);

    const result = await listRecentNovelties({
      authUser: req.user,
      days,
      limit
    });

    return res.json(result);
  } catch (e) {
    console.error("[RECENT NOVELTIES]", e);
    return res.status(Number(e?.status) || 500).json({
      ok: false,
      error: e?.message || "No se pudo listar novedades recientes"
    });
  }
}

export async function getNoveltyByIdController(req, res) {
  try {
    const id = Number(req.params.id);
    if (!id) return res.status(400).json({ ok: false, error: "ID inválido" });

    const result = await getNoveltyById({ authUser: req.user, id });
    return res.json(result);
  } catch (e) {
    console.error("[GET NOVELTY BY ID]", e);
    return res.status(Number(e?.status) || 500).json({
      ok: false,
      error: e?.message || "No se pudo obtener la novedad"
    });
  }
}

export async function updateNoveltyController(req, res) {
  try {
    const id = Number(req.params.id);
    if (!id) return res.status(400).json({ ok: false, error: "ID inválido" });

    const { novelty_type, start_date, end_date, notes } = req.body;

    const result = await updateNovelty({
      authUser: req.user,
      id,
      patch: { novelty_type, start_date, end_date, notes }
    });

    return res.json(result);
  } catch (e) {
    console.error("[UPDATE NOVELTY]", e);
    return res.status(Number(e?.status) || 500).json({
      ok: false,
      error: e?.message || "No se pudo actualizar la novedad",
      code: e?.code,
      overlaps: e?.overlaps
    });
  }
}

export async function deleteNoveltyController(req, res) {
  try {
    const id = Number(req.params.id);
    if (!id) return res.status(400).json({ ok: false, error: "ID inválido" });

    const result = await deleteNovelty({ authUser: req.user, id });
    return res.json(result);
  } catch (e) {
    console.error("[DELETE NOVELTY]", e);
    return res.status(Number(e?.status) || 500).json({
      ok: false,
      error: e?.message || "No se pudo eliminar la novedad"
    });
  }
}

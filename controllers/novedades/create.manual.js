// controllers/novedades/create.manual.js
import { createNoveltyManual } from "../../services/novedades.manual.service.js";

export async function createNoveltyManualController(req, res) {
  try {
    const {
      user_id,
      document_id,
      name,
      novelty_type,
      start_date,
      end_date,
      notes
    } = req.body;

    // Mínimo: alguna forma de identificar usuario
    if (!user_id && !document_id && !name) {
      return res.status(400).json({
        ok: false,
        error: "Debes enviar user_id o document_id o name para identificar el usuario."
      });
    }

    const result = await createNoveltyManual({
      user_id,
      document_id,
      name,
      novelty_type: novelty_type || "NOVEDAD",
      start_date,
      end_date,
      notes
    });

    // createNoveltyManual retorna { ok:true, novelty, monthly_updates, progress_recalc? }
    return res.status(201).json(result);
  } catch (e) {
    console.error("[CREATE NOVEDAD MANUAL]", e);

    // Respeta status si el service lo define (409 overlap/duplicate, 404 user not found, etc.)
    const status = Number(e?.status) || 400;

    // Incluimos overlaps si el service los expone para que el front muestre conflicto
    const payload = {
      ok: false,
      error: e?.message || "No se pudo crear la novedad"
    };

    if (e?.code) payload.code = e.code;
    if (e?.overlaps) payload.overlaps = e.overlaps;

    return res.status(status).json(payload);
  }
}

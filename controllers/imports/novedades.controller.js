// controllers/imports/novedades.controller.js
import { importNovedadesToStaging } from "../../services/import.novedades.service.js";

export async function importNovedadesController(req, res) {
  try {
    // En este endpoint usas multer.memoryStorage(), por eso NO hay req.file.path.
    // El archivo viene en req.file.buffer.
    if (!req.file?.buffer) {
      return res.status(400).json({
        ok: false,
        error: "Debes adjuntar un archivo Excel en 'file'."
      });
    }

    const actor_user_id = req.user?.id ?? null;

    const result = await importNovedadesToStaging({
      buffer: req.file.buffer,
      sourceFilename: req.file.originalname || null,
      mimetype: req.file.mimetype || null,
      actor_user_id
    });

    return res.json({ ok: true, ...result });
  } catch (e) {
    console.error("[IMPORT NOVEDADES]", e);
    return res.status(400).json({
      ok: false,
      error: e?.message || "No se pudo importar novedades"
    });
  }
}

// controllers/imports/presupuesto.js
import { importPresupuestoToStaging } from "../../services/import.presupuesto.service.js";

export async function importPresupuesto(req, res) {
  try {
    if (!req.file) {
      return res.status(400).json({ ok: false, error: "Falta archivo (form-data key: file)" });
    }

    const result = await importPresupuestoToStaging({
      buffer: req.file.buffer,
      originalName: req.file.originalname,
    });

    return res.json({
      ok: true,
      dataset: "presupuesto",
      table: "staging.archivo_presupuesto",
      ...result,
    });
  } catch (e) {
    console.error("[IMPORT presupuesto]", e);
    return res.status(500).json({
      ok: false,
      error: "No se pudo importar presupuesto",
      detail: e.message,
    });
  }
}

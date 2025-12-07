// controllers/imports/siapp_full.js
import { importSiappFull } from '../../services/imports.siapp_full.service.js'

export async function importSiappFullController(req, res) {
  try {
    const file = req.file;
    if (!file) return res.status(400).json({ error: "No se recibió archivo" });

    const result = await importSiappFull({
      file,
      source_file: file.originalname
    });

    return res.json(result);

  } catch (e) {
    console.error("[IMPORT siapp_full]", e);
    return res.status(400).json({ error: e.message });
  }
}

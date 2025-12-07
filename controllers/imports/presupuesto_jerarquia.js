// controllers/imports/presupuesto_jerarquia.js
import { importPresupuestoJerarquia } from "../../services/imports.presupuesto_jerarquia.service.js";

export async function importPresupuestoJerarquiaController(req, res) {
  try {
    // ------------------------------
    // 1. Validar archivo recibido
    // ------------------------------
    if (!req.file) {
      return res.status(400).json({
        ok: false,
        error: "Debes enviar un archivo Excel en form-data con el campo 'file'"
      });
    }

    console.log("[IMPORT PJ] Archivo recibido:", {
      originalname: req.file.originalname,
      size: req.file.size
    });

    // ------------------------------
    // 2. Ejecutar importador
    // ------------------------------
    const result = await importPresupuestoJerarquia(req.file.buffer);

    // ------------------------------
    // 3. Responder al cliente
    // ------------------------------
    return res.json({
      ok: true,
      ...result
    });

  } catch (error) {
    console.error("[IMPORT PJ] Error al importar Presupuesto Jerarquía:", error);

    return res.status(500).json({
      ok: false,
      error: "No se pudo importar Presupuesto Jerarquía",
      detail: error.message
    });
  }
}

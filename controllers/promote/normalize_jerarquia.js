// controllers/promote/normalize_jerarquia.js
import { normalizeJerarquia } from "../../services/promote.normalize_jerarquia.service.js";

export async function normalizeJerarquiaController(req, res) {
  try {
    console.log("▶ Ejecutando normalización de jerarquía...");

    const result = await normalizeJerarquia();

    return res.status(200).json({
      ok: true,
      message: result.message,
      resumen: result.resumen
    });

  } catch (e) {
    console.error(" Error al normalizar jerarquía:", e);
    return res.status(500).json({
      ok: false,
      message: "Error interno al normalizar jerarquía",
      error: e.message
    });
  }
}

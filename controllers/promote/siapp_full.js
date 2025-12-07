// controllers/promote/siapp_full.js
import { promoteSiappFull } from '../../services/promote.siapp_full.service.js';

/**
 * Controller para promover SIAPP FULL desde staging → siapp.full_sales
 * Recibe:  ?period=YYYY-MM  y opcional ?source_file=<nombre>
 */
export async function promoteSiappFULL(req, res) {
  try {
    const { period, source_file = null } = req.query;

    // --------------------------
    // 1) Validación del periodo
    // --------------------------
    if (!period) {
      return res.status(400).json({ error: "Falta ?period=YYYY-MM" });
    }

    const match = String(period).trim().match(/^(\d{4})-(\d{1,2})$/);
    if (!match) {
      return res.status(400).json({ error: "Periodo inválido. Use formato YYYY-MM" });
    }

    const year = Number(match[1]);
    const month = Number(match[2]);

    if (year < 2000 || month < 1 || month > 12) {
      return res.status(400).json({ error: "Periodo fuera de rango válido" });
    }

    // --------------------------
    // 2) Ejecutar promoción
    // --------------------------
    const result = await promoteSiappFull({
      period_year: year,
      period_month: month,
      source_file
    });

    return res.json({
      ok: true,
      message: "Promoción SIAPP FULL realizada correctamente",
      period: `${year}-${String(month).padStart(2, "0")}`,
      ...result
    });

  } catch (e) {
    console.error("[PROMOTE siapp_full]", e);

    // --------------------------
    // 3) Errores controlados
    // --------------------------
    if (e.message.includes("No hay datos en staging")) {
      return res.status(400).json({ error: "No hay datos en staging.siapp_full" });
    }

    if (e.message.includes("permission denied")) {
      return res.status(403).json({ error: "Permiso denegado al esquema siapp" });
    }

    if (e.message.includes("full_sales")) {
      return res.status(500).json({
        error: "Error al escribir en siapp.full_sales",
        detail: e.message
      });
    }

    // --------------------------
    // 4) Error genérico
    // --------------------------
    return res.status(500).json({
      error: "Error interno al promover SIAPP FULL",
      detail: e.message
    });
  }
}

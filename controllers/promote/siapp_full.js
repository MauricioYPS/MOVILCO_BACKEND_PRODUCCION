// ======================================================================
//  PROMOTE — SIAPP FULL (VERSIÓN FINAL 2025-12 SIN COLUMNA DATA)
//  Este controlador utiliza el servicio correcto que hace backup REAL
// ======================================================================

import { promoteSiappFull } from "../../services/promote.siapp_full.service.js";

export async function promoteSiappFULL(req, res) {
  try {
    const { period, source_file = null } = req.query;

    // --------------------------
    // 1. Validar periodo
    // --------------------------
    if (!period) {
      return res.status(400).json({ ok: false, error: "Falta ?period=YYYY-MM" });
    }

    const match = String(period).trim().match(/^(\d{4})-(\d{1,2})$/);
    if (!match) {
      return res.status(400).json({
        ok: false,
        error: "El formato del periodo es inválido. Use YYYY-MM"
      });
    }

    const year = Number(match[1]);
    const month = Number(match[2]);

    if (year < 2000 || month < 1 || month > 12) {
      return res.status(400).json({
        ok: false,
        error: "Periodo fuera de rango válido"
      });
    }

    // --------------------------
    // 2. Ejecutar PROMOTE real
    // --------------------------
    const result = await promoteSiappFull({
      period_year: year,
      period_month: month,
      source_file
    });

    return res.json({
      ok: true,
      message: "Promoción SIAPP FULL ejecutada correctamente",
      ...result
    });

  } catch (error) {
    console.error("[PROMOTE SIAPP FULL ERROR]", error);

    return res.status(500).json({
      ok: false,
      error: "Error interno al promover SIAPP FULL",
      detail: error.message
    });
  }
}

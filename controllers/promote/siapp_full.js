// ======================================================================
//  PROMOTE — SIAPP FULL (ACTUALIZADO)
//  Cambios:
//   - El endpoint YA NO exige ?period=YYYY-MM
//   - period es OPCIONAL:
//       * Si viene period=YYYY-MM -> promueve solo ese mes (desde staging)
//       * Si NO viene period      -> promueve TODOS los meses presentes en staging
//  Nota:
//   - El service promoteSiappFull ahora recibe { source_file, period } (period opcional)
// ======================================================================

import { promoteSiappFull } from "../../services/promote.siapp_full.service.js";

export async function promoteSiappFULL(req, res) {
  try {
    // period opcional: puede venir como period o month (compatibilidad)
    const period = req.query.period ?? req.query.month ?? null;
    const source_file = req.query.source_file ?? null;

    // --------------------------
    // 1. Validar periodo (SOLO si viene)
    // --------------------------
    let cleanPeriod = null;

    if (period) {
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

      cleanPeriod = `${year}-${String(month).padStart(2, "0")}`;
    }

    // --------------------------
    // 2. Ejecutar PROMOTE
    //    - Si cleanPeriod es null: promueve TODO staging (multi-mes)
    //    - Si cleanPeriod existe:  promueve SOLO ese mes
    // --------------------------
    const result = await promoteSiappFull({
      source_file,
      period: cleanPeriod
    });

    return res.json({
      ok: true,
      message: cleanPeriod
        ? `Promoción SIAPP FULL ejecutada correctamente para ${cleanPeriod}`
        : "Promoción SIAPP FULL ejecutada correctamente (multi-mes desde staging)",
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

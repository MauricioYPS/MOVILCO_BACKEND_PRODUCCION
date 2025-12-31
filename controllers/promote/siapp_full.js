// ======================================================================
// CONTROLLER — PROMOTE SIAPP FULL + RECALC PROGRESS (AUTO)
// 2025-12-28
//
// POST /api/promote/siapp/full
// Query params:
//   - mode=merge|rebuild   (default: rebuild)
//   - period=YYYY-MM       (opcional; si viene procesa solo ese mes)
//   - source_file=...      (opcional; fallback para source_file)
//
// Flujo:
//   1) promoteSiappFull({ source_file, period, mode })
//   2) recalcula core.progress solo para los meses tocados
//      usando promoteSiappFromFullSales({ period_year, period_month })
// ======================================================================

import { promoteSiappFull } from "../../services/promote.siapp_full.service.js";
import { promoteSiappFromFullSales } from "../../services/promote.siapp.service.js";

function parsePeriodOptional(period) {
  if (!period) return null;

  const match = String(period).trim().match(/^(\d{4})-(\d{1,2})$/);
  if (!match) return { error: "El formato del periodo es inválido. Use YYYY-MM" };

  const year = Number(match[1]);
  const month = Number(match[2]);

  if (!Number.isFinite(year) || !Number.isFinite(month) || year < 2000 || month < 1 || month > 12) {
    return { error: "Periodo fuera de rango válido" };
  }

  return { value: `${year}-${String(month).padStart(2, "0")}` };
}

function normalizeMode(mode) {
  const m = String(mode || "rebuild").trim().toLowerCase();
  if (m === "merge" || m === "rebuild") return m;
  return "rebuild";
}

// extrae meses desde el resultado del service de forma robusta
function extractTouchedMonths(result) {
  // Tu service actual retorna: meses_detectados_en_staging
  // Versiones anteriores retornaban: meses_insertados
  const raw =
    result?.meses_detectados_en_staging ||
    result?.meses_insertados ||
    result?.months ||
    [];

  if (!Array.isArray(raw)) return [];

  // Normaliza estructura {period_year, period_month}
  return raw
    .map((x) => ({
      period_year: Number(x.period_year),
      period_month: Number(x.period_month),
      filas: x.filas != null ? Number(x.filas) : null
    }))
    .filter((x) => Number.isFinite(x.period_year) && Number.isFinite(x.period_month));
}

export async function promoteSiappFULL(req, res) {
  try {
    const mode = normalizeMode(req.query.mode);
    const source_file = req.query.source_file ?? null;

    // period opcional (compatibilidad: period o month)
    const periodRaw = req.query.period ?? req.query.month ?? null;
    const parsed = parsePeriodOptional(periodRaw);

    if (parsed?.error) {
      return res.status(400).json({ ok: false, error: parsed.error });
    }

    const cleanPeriod = parsed?.value ?? null;

    // ----------------------------------------------------
    // 1) PROMOTE full_sales desde staging (merge o rebuild)
    // ----------------------------------------------------
    const promoteResult = await promoteSiappFull({
      source_file,
      period: cleanPeriod,
      mode
    });

    // ----------------------------------------------------
    // 2) Recalcular progress para meses tocados
    // ----------------------------------------------------
    const months = extractTouchedMonths(promoteResult);

    // Si el usuario pidió explícitamente un period, forzamos ese mes
    // (aunque por alguna razón el detector no lo devolviera)
    let monthsToRecalc = months;

    if (cleanPeriod) {
      const [y, m] = cleanPeriod.split("-").map(Number);
      monthsToRecalc = [{ period_year: y, period_month: m }];
    }

    if (!monthsToRecalc.length) {
      // No es error fatal; devolvemos el promote tal cual.
      return res.json({
        ok: true,
        message: cleanPeriod
          ? `Promoción SIAPP FULL ejecutada para ${cleanPeriod}. No se detectaron meses para recalcular progress.`
          : "Promoción SIAPP FULL ejecutada. No se detectaron meses para recalcular progress.",
        promote: promoteResult,
        progress_recalc: {
          total_months: 0,
          total_upserted: 0,
          results: []
        }
      });
    }

    const progressResults = [];
    let totalUpserted = 0;

    // Secuencial para evitar carga DB
    for (const p of monthsToRecalc) {
      const r = await promoteSiappFromFullSales({
        period_year: p.period_year,
        period_month: p.period_month
      });

      progressResults.push({
        period: `${p.period_year}-${String(p.period_month).padStart(2, "0")}`,
        period_year: p.period_year,
        period_month: p.period_month,
        total_sales_rows: r.total_sales_rows,
        total_asesores_en_siapp: r.total_asesores_en_siapp,
        matched_users: r.matched_users,
        upserted: r.upserted
      });

      totalUpserted += Number(r.upserted || 0);
    }

    // ----------------------------------------------------
    // 3) Response final
    // ----------------------------------------------------
    return res.json({
      ok: true,
      message: cleanPeriod
        ? `Promoción SIAPP FULL + recálculo progress ejecutado para ${cleanPeriod} (mode=${mode})`
        : `Promoción SIAPP FULL + recálculo progress ejecutado (mode=${mode})`,
      mode,
      period: cleanPeriod,
      promote: promoteResult,
      progress_recalc: {
        total_months: progressResults.length,
        total_upserted: totalUpserted,
        results: progressResults
      }
    });
  } catch (error) {
    console.error("[PROMOTE SIAPP FULL + RECALC ERROR]", error);
    return res.status(500).json({
      ok: false,
      error: "Error interno al promover SIAPP FULL y recalcular progress",
      detail: error.message
    });
  }
}

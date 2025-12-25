// controllers/promote/siapp.batch.js
import { listSiappPeriods } from "../../services/siapp.periods.service.js";
import { promoteSiappFromFullSales } from "../../services/promote.siapp.service.js";

/**
 * POST /api/promote/siapp/batch
 * - Recalcula core.progress para TODOS los periodos existentes en siapp.full_sales.
 * - Opcional: filtrar por year con ?year=2025
 * - Opcional: filtrar por rango con ?from=YYYY-MM&to=YYYY-MM
 */
export async function promoteSiappBatch(req, res) {
  try {
    const yearFilter = req.query.year ? Number(req.query.year) : null;
    const from = req.query.from ? String(req.query.from).trim() : null; // YYYY-MM
    const to = req.query.to ? String(req.query.to).trim() : null;       // YYYY-MM

    let periods = await listSiappPeriods();

    // Filtrar por año si se envía
    if (yearFilter) {
      periods = periods.filter(p => p.period_year === yearFilter);
    }

    // Filtrar por rango (comparación lexicográfica funciona para YYYY-MM)
    if (from) {
      periods = periods.filter(p => p.period >= from);
    }
    if (to) {
      periods = periods.filter(p => p.period <= to);
    }

    if (periods.length === 0) {
      return res.status(400).json({
        ok: false,
        error: "No hay periodos para procesar con los filtros enviados"
      });
    }

    const results = [];
    let totalUpserted = 0;

    // Secuencial (más seguro para BD). Si luego quieres paralelizar, se hace con límite de concurrencia.
    for (const p of periods) {
      const r = await promoteSiappFromFullSales({
        period_year: p.period_year,
        period_month: p.period_month
      });

      // tu service ya retorna ok/contadores
      results.push({
        period: p.period,
        period_year: p.period_year,
        period_month: p.period_month,
        total_sales_rows: r.total_sales_rows,
        total_asesores_en_siapp: r.total_asesores_en_siapp,
        matched_users: r.matched_users,
        upserted: r.upserted
      });

      totalUpserted += Number(r.upserted || 0);
    }

    return res.json({
      ok: true,
      total_periods: results.length,
      total_upserted: totalUpserted,
      results
    });
  } catch (e) {
    console.error("[PROMOTE SIAPP BATCH]", e);
    return res.status(500).json({
      ok: false,
      error: "No se pudo ejecutar el batch de SIAPP",
      detail: e.message
    });
  }
}

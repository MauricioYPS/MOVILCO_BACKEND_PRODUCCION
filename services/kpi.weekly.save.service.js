// ======================================================================
// KPI WEEKLY SAVE SERVICE — Versión Final 2025-12
// ======================================================================

import pool from "../config/database.js";
import { calculateWeeklyKpi } from "./kpi.weekly.calculate.service.js";

/***********************************************************************
 * GUARDADO DEL KPI SEMANAL
 ***********************************************************************/
export async function saveWeeklyKpi(period) {
  console.log(`\n🟦 [WEEKLY SAVE] Iniciando guardado semanal para periodo ${period}`);

  // 1. Calcular el KPI semanal
  const calc = await calculateWeeklyKpi(period);
  const results = calc.results;

  if (!results.length)
    return { ok: false, message: "No hay datos para guardar." };

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // 2. Agrupar por semana para eliminar antes de insertar
    const semanas = {};
    for (const r of results) {
      const key = `${r.year}-${r.week_number}`;
      if (!semanas[key]) semanas[key] = [];
      semanas[key].push(r);
    }

    // 3. DELETE por cada semana encontrada
    for (const key of Object.keys(semanas)) {
      const [year, week_number] = key.split("-").map(n => Number(n));
      await client.query(
        `
        DELETE FROM kpi.weekly_kpi_results
        WHERE year = $1 AND week_number = $2
        `,
        [year, week_number]
      );
      console.log(`🗑 Semana eliminada antes de insertar: ${key}`);
    }

    // 4. INSERTAR NUEVOS REGISTROS
    const insertQuery = `
      INSERT INTO kpi.weekly_kpi_results (
        week_start, week_end, year, week_number,
        asesor_id, documento, nombre, estado,
        org_unit_id, distrito_claro,
        dias_laborados, presupuesto_prorrateado,
        ventas_distrito, ventas_fuera, ventas_totales,
        cumple_distrito, cumple_global,
        raw_json
      )
      VALUES (
        $1,$2,$3,$4,
        $5,$6,$7,$8,
        $9,$10,
        $11,$12,
        $13,$14,$15,
        $16,$17,
        $18
      )
    `;

    let inserted = 0;

    for (const r of results) {
      const raw = { ...r };

      await client.query(insertQuery, [
        r.week_start,
        r.week_end,
        r.year,
        r.week_number,

        r.asesor_id,
        r.documento,
        r.nombre,
        r.estado,

        r.org_unit_id,
        r.distrito_claro,

        null, // dias_laborados (no aplica semanal aún)
        null, // presupuesto_prorrateado (no aplica semanal)

        r.ventas_distrito,
        r.ventas_fuera,
        r.ventas_totales,

        null, // cumple_distrito (no aplica)
        null, // cumple_global (no aplica)

        raw
      ]);

      inserted++;
    }

    await client.query("COMMIT");
    console.log(`\n✅ [WEEKLY SAVE] Guardado exitoso: ${inserted} registros insertados\n`);

    return {
      ok: true,
      message: "Weekly KPI guardado correctamente",
      registros_insertados: inserted,
      semanas: Object.keys(semanas).length
    };

  } catch (e) {
    await client.query("ROLLBACK");
    console.error("\n❌ [WEEKLY SAVE ERROR]", e);
    throw e;
  } finally {
    client.release();
  }
}

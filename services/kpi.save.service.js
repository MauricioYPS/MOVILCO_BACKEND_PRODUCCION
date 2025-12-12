import pool from "../config/database.js";
import { calculateKpiForPeriod } from "./kpi.calculate.service.js";

/**
 * Guarda el KPI mensual dentro de kpi.monthly_kpi_results.
 *
 * REGLAS:
 *  - Recalcula KPI siempre
 *  - Borra registros del mismo periodo
 *  - Inserta KPI limpio y actualizado
 *  - raw_json contiene estructura completa del asesor
 */
export async function saveKpiForPeriod(period) {
  console.log(`\n[KPI SAVE] Iniciando guardado para periodo ${period}`);

  // 1) Calcular KPI del periodo (ya incluye usuarios, ventas y presupuesto)
  const calc = await calculateKpiForPeriod(period);
  const { year, month } = calc.periodo;
  const kpis = calc.kpis;

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    /************************************************************
     * 1. BORRAR KPI PREVIO DEL MISMO PERIODO
     ************************************************************/
    const del = await client.query(
      `
      DELETE FROM kpi.monthly_kpi_results
      WHERE period_year = $1 AND period_month = $2
      `,
      [year, month]
    );

    console.log(`[KPI SAVE] Eliminados ${del.rowCount} registros previos.`);

    /************************************************************
     * 2. INSERTAR KPI NUEVO (estructura final)
     ************************************************************/
    const insertQuery = `
      INSERT INTO kpi.monthly_kpi_results (
        period_year, period_month,
        asesor_id, documento, nombre, estado,
        org_unit_id, distrito_claro,
        dias_laborados, presupuesto_prorrateado,
        ventas_distrito, ventas_fuera, ventas_totales,
        cumple_distrito, cumple_global,
        raw_json
      )
      VALUES (
        $1,$2,
        $3,$4,$5,$6,
        $7,$8,
        $9,$10,
        $11,$12,$13,
        $14,$15,
        $16
      )
    `;

    let insertados = 0;

    for (const k of kpis) {
      const raw = {
        ...k,
        periodo: { year, month }
      };

      await client.query(insertQuery, [
        year,
        month,

        k.asesor_id,
        k.documento,
        k.nombre,
        k.estado,

        k.org_unit_id ?? null,
        k.distrito_claro ?? null,

        k.dias_laborados,
        k.presupuesto_prorrateado,

        k.ventas_distrito,
        k.ventas_fuera,
        k.ventas_totales,

        k.cumple_distrito,
        k.cumple_global,

        raw
      ]);

      insertados++;
    }

    await client.query("COMMIT");

    console.log(
      `[KPI SAVE] Guardado exitoso: ${insertados} registros insertados`
    );

    return {
      ok: true,
      message: "KPI guardado correctamente",
      period,
      saved: insertados,
      total_ventas_reales: calc.total_ventas_reales
    };

  } catch (e) {
    await client.query("ROLLBACK");
    console.error("\n[ KPI SAVE ERROR]", e);
    throw e;

  } finally {
    client.release();
  }
}

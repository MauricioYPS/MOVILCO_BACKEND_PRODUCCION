// ======================================================================
// PROMOTE SIAPP FULL — VERSIÓN ACTUALIZADA 2025-12-24 (ROBUSTA)
// Objetivo:
//   - NO volver a “aplanar” todas las ventas a un solo periodo.
//   - Guardar MULTI-MES en siapp.full_sales usando periodo derivado de FECHA.
//   - NO truncar toda full_sales: reemplaza solo los meses presentes en staging
//     (o solo el mes solicitado si se envía period=YYYY-MM).
// Mantiene:
//   - cantserv como VARCHAR (NO numeric)
//   - casteo correcto de campos numeric reales
//   - backup liviano y ordenado en historico.siapp_full_backup
//
// Mejora clave para evitar errores:
//   - Se usa fecha_date = NULLIF(fecha::text,'')::date para soportar fecha como TEXT/DATE
//   - Se omiten filas con fecha inválida y se reportan en el retorno
// ======================================================================

import pool from "../config/database.js";

function parsePeriod(period) {
  if (!period) return null;
  const m = String(period).trim().match(/^(\d{4})-(\d{2})$/);
  if (!m) return null;
  const year = Number(m[1]);
  const month = Number(m[2]);
  if (month < 1 || month > 12) return null;
  return { year, month };
}

export async function promoteSiappFull({ source_file = null, period = null } = {}) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // ----------------------------------------------------
    // 1. Validar que staging tiene datos
    // ----------------------------------------------------
    const check = await client.query(`SELECT COUNT(*) AS total FROM staging.siapp_full`);
    const totalStaging = Number(check.rows[0]?.total || 0);
    if (totalStaging === 0) {
      throw new Error("No hay datos en staging.siapp_full");
    }

    const periodFilter = parsePeriod(period);
    if (period && !periodFilter) {
      throw new Error("Formato de period inválido. Use YYYY-MM.");
    }

    // ----------------------------------------------------
    // 1.1 Contar filas con fecha inválida (para trazabilidad)
    //      Nota: si fecha no puede castear a date -> queda NULL en fecha_date.
    // ----------------------------------------------------
    const invalidQ = await client.query(
      `
      SELECT COUNT(*)::bigint AS invalid_fecha
      FROM staging.siapp_full fs
      WHERE NULLIF(fs.fecha::text,'')::date IS NULL
      `
    );
    const invalidFecha = Number(invalidQ.rows[0]?.invalid_fecha || 0);

    // ----------------------------------------------------
    // 2. Determinar meses a procesar (para logging/resultado)
    //    Usamos fecha_date robusta
    // ----------------------------------------------------
    const monthsQ = await client.query(
      `
      WITH base AS (
        SELECT
          NULLIF(fecha::text,'')::date AS fecha_date
        FROM staging.siapp_full
      )
      SELECT
        EXTRACT(YEAR FROM fecha_date)::int  AS period_year,
        EXTRACT(MONTH FROM fecha_date)::int AS period_month,
        COUNT(*)::bigint AS filas
      FROM base
      WHERE fecha_date IS NOT NULL
        AND ($1::int IS NULL OR EXTRACT(YEAR FROM fecha_date)::int = $1)
        AND ($2::int IS NULL OR EXTRACT(MONTH FROM fecha_date)::int = $2)
      GROUP BY 1,2
      ORDER BY 1,2
      `,
      [periodFilter?.year ?? null, periodFilter?.month ?? null]
    );

    const months = monthsQ.rows.map(r => ({
      period_year: Number(r.period_year),
      period_month: Number(r.period_month),
      filas: Number(r.filas)
    }));

    if (months.length === 0) {
      throw new Error(
        periodFilter
          ? `No hay filas en staging.siapp_full para el periodo ${periodFilter.year}-${String(periodFilter.month).padStart(2, "0")} (o todas tienen fecha inválida)`
          : "No hay filas válidas con fecha en staging.siapp_full"
      );
    }

    // Un backup “run id” único
    const periodo_backup = `${Date.now()}`;

    // ----------------------------------------------------
    // 3. BACKUP (multi-mes): periodo_comercial por fila (derivado de fecha_date)
    // ----------------------------------------------------
    await client.query(
      `
      INSERT INTO historico.siapp_full_backup (
        periodo_comercial,
        periodo_backup,

        estado_liquidacion, linea_negocio, cuenta, ot,
        idasesor, nombreasesor, cantserv, tipored, division,
        area, zona, poblacion, d_distrito, renta, fecha, venta,
        tipo_registro, estrato, paquete_pvd, mintic, tipo_prodcuto,
        venta_instale_dth, sac_final,
        cedula_vendedor, nombre_vendedor, modalidad_venta,
        tipo_vendedor, tipo_red_comercial, nombre_regional,
        nombre_comercial, nombre_lider, retencion_control,
        observ_retencion, tipo_contrato,
        tarifa_venta, comision_neta, punto_equilibrio,
        source_file
      )
      SELECT
        TO_CHAR(NULLIF(fs.fecha::text,'')::date, 'YYYY-MM') AS periodo_comercial,
        $1 AS periodo_backup,

        fs.estado_liquidacion,
        fs.linea_negocio,
        fs.cuenta,
        fs.ot,
        fs.idasesor,
        fs.nombreasesor,
        fs.cantserv,                            -- VARCHAR
        fs.tipored,
        fs.division,
        fs.area,
        fs.zona,
        fs.poblacion,
        fs.d_distrito,

        NULLIF(fs.renta, '')::numeric,          -- CAST CORRECTO
        NULLIF(fs.fecha::text,'')::date,        -- fecha segura
        fs.venta,
        fs.tipo_registro,
        fs.estrato,
        fs.paquete_pvd,
        fs.mintic,
        fs.tipo_prodcuto,
        fs.venta_instale_dth,
        fs.sac_final,
        fs.cedula_vendedor,
        fs.nombre_vendedor,
        fs.modalidad_venta,
        fs.tipo_vendedor,
        fs.tipo_red_comercial,
        fs.nombre_regional,
        fs.nombre_comercial,
        fs.nombre_lider,
        fs.retencion_control,
        fs.observ_retencion,
        fs.tipo_contrato,

        NULLIF(fs.tarifa_venta, '')::numeric,    -- CAST
        NULLIF(fs.comision_neta, '')::numeric,   -- CAST
        NULLIF(fs.punto_equilibrio, '')::numeric,-- CAST

        fs.source_file
      FROM staging.siapp_full fs
      WHERE NULLIF(fs.fecha::text,'')::date IS NOT NULL
        AND ($2::int IS NULL OR EXTRACT(YEAR FROM NULLIF(fs.fecha::text,'')::date)::int = $2)
        AND ($3::int IS NULL OR EXTRACT(MONTH FROM NULLIF(fs.fecha::text,'')::date)::int = $3)
      `,
      [periodo_backup, periodFilter?.year ?? null, periodFilter?.month ?? null]
    );

    console.log(`✔ Backup SIAPP FULL creado. periodo_backup=${periodo_backup}`);

    // ----------------------------------------------------
    // 4. Limpiar destino SOLO para los meses a insertar
    // ----------------------------------------------------
    if (periodFilter) {
      await client.query(
        `DELETE FROM siapp.full_sales WHERE period_year = $1 AND period_month = $2`,
        [periodFilter.year, periodFilter.month]
      );
    } else {
      await client.query(`
        WITH months AS (
          SELECT DISTINCT
            EXTRACT(YEAR FROM NULLIF(fecha::text,'')::date)::int  AS y,
            EXTRACT(MONTH FROM NULLIF(fecha::text,'')::date)::int AS m
          FROM staging.siapp_full
          WHERE NULLIF(fecha::text,'')::date IS NOT NULL
        )
        DELETE FROM siapp.full_sales fs
        USING months mo
        WHERE fs.period_year = mo.y AND fs.period_month = mo.m
      `);
    }

    // ----------------------------------------------------
    // 5. Insertar desde staging → full_sales
    //    (period_year/month derivado de fecha_date robusta)
    // ----------------------------------------------------
    const insertResult = await client.query(
      `
      INSERT INTO siapp.full_sales (
        period_year, period_month,
        estado_liquidacion, linea_negocio, cuenta, ot,
        idasesor, nombreasesor, cantserv, tipored, division,
        area, zona, poblacion, d_distrito,
        renta, fecha, venta,
        tipo_registro, estrato, paquete_pvd, mintic, tipo_prodcuto,
        ventaconvergente, venta_instale_dth, sac_final,
        cedula_vendedor, nombre_vendedor, modalidad_venta,
        tipo_vendedor, tipo_red_comercial, nombre_regional,
        nombre_comercial, nombre_lider, retencion_control,
        observ_retencion, tipo_contrato,
        tarifa_venta, comision_neta, punto_equilibrio,
        raw_json, source_file
      )
      SELECT
        EXTRACT(YEAR FROM NULLIF(fecha::text,'')::date)::int,
        EXTRACT(MONTH FROM NULLIF(fecha::text,'')::date)::int,

        estado_liquidacion,
        linea_negocio,
        cuenta,
        ot,
        idasesor,
        nombreasesor,

        cantserv,                     -- VARCHAR (NO CAST)

        tipored,
        division,
        area,
        zona,
        poblacion,
        d_distrito,

        NULLIF(renta,'')::numeric,
        NULLIF(fecha::text,'')::date,  -- fecha segura
        venta,

        tipo_registro,
        estrato,
        paquete_pvd,
        mintic,
        tipo_prodcuto,
        ventaconvergente,
        venta_instale_dth,
        sac_final,
        cedula_vendedor,
        nombre_vendedor,
        modalidad_venta,
        tipo_vendedor,
        tipo_red_comercial,
        nombre_regional,
        nombre_comercial,
        nombre_lider,
        retencion_control,
        observ_retencion,
        tipo_contrato,

        NULLIF(tarifa_venta,'')::numeric,
        NULLIF(comision_neta,'')::numeric,
        NULLIF(punto_equilibrio,'')::numeric,

        raw_json,
        COALESCE(source_file, $1)
      FROM staging.siapp_full
      WHERE NULLIF(fecha::text,'')::date IS NOT NULL
        AND ($2::int IS NULL OR EXTRACT(YEAR FROM NULLIF(fecha::text,'')::date)::int = $2)
        AND ($3::int IS NULL OR EXTRACT(MONTH FROM NULLIF(fecha::text,'')::date)::int = $3)
      RETURNING 1
      `,
      [source_file, periodFilter?.year ?? null, periodFilter?.month ?? null]
    );

    const total_insertados = insertResult.rowCount;

    await client.query("COMMIT");

    return {
      ok: true,
      periodo_backup,
      filtro_periodo: periodFilter
        ? `${periodFilter.year}-${String(periodFilter.month).padStart(2, "0")}`
        : null,
      meses_insertados: months,
      total_staging: totalStaging,
      total_insertados,
      filas_con_fecha_invalida_en_staging: invalidFecha
    };
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("[PROMOTE SIAPP FULL ERROR]", err);
    throw err;
  } finally {
    client.release();
  }
}

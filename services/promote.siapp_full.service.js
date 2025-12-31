// ======================================================================
// PROMOTE SIAPP FULL — 2025-12-28
// MODOS:
//  - mode="rebuild": borra y reemplaza SOLO los meses presentes en staging
//                   (o solo el mes solicitado con period=YYYY-MM).
//  - mode="merge":   inserta incremental (diario) usando OT como llave única:
//                   INSERT ... ON CONFLICT (ot) DO UPDATE.
// REGLAS:
//  - period_year/month SIEMPRE derivado de fecha (no aplanar a un solo periodo)
//  - NO se insertan filas con fecha inválida
//  - Backup multi-mes en historico.siapp_full_backup
//  - cantserv es VARCHAR (no cast)
//  - casteo numérico solo en campos numeric reales
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

function normalizeMode(mode) {
  const m = String(mode || "rebuild").trim().toLowerCase();
  if (m === "merge" || m === "rebuild") return m;
  return "rebuild";
}

export async function promoteSiappFull({
  source_file = null,
  period = null,      // "YYYY-MM" opcional
  mode = "rebuild"    // "rebuild" | "merge"
} = {}) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // ----------------------------------------------------
    // 1) Validaciones base
    // ----------------------------------------------------
    const check = await client.query(`SELECT COUNT(*)::bigint AS total FROM staging.siapp_full`);
    const totalStaging = Number(check.rows[0]?.total || 0);
    if (totalStaging === 0) {
      throw new Error("No hay datos en staging.siapp_full");
    }

    const periodFilter = parsePeriod(period);
    if (period && !periodFilter) {
      throw new Error("Formato de period inválido. Use YYYY-MM.");
    }

    const safeMode = normalizeMode(mode);

    // ----------------------------------------------------
    // 1.1) Contar filas con fecha inválida
    // ----------------------------------------------------
    const invalidQ = await client.query(`
      SELECT COUNT(*)::bigint AS invalid_fecha
      FROM staging.siapp_full fs
      WHERE NULLIF(fs.fecha::text,'')::date IS NULL
    `);
    const invalidFecha = Number(invalidQ.rows[0]?.invalid_fecha || 0);

    // ----------------------------------------------------
    // 2) Determinar meses a procesar (según fecha)
    // ----------------------------------------------------
    const monthsQ = await client.query(
      `
      WITH base AS (
        SELECT NULLIF(fecha::text,'')::date AS fecha_date
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
          ? `No hay filas válidas con fecha para el periodo ${periodFilter.year}-${String(periodFilter.month).padStart(2, "0")} en staging.siapp_full`
          : "No hay filas válidas con fecha en staging.siapp_full"
      );
    }

    // ----------------------------------------------------
    // 3) BACKUP multi-mes (siempre)
    // ----------------------------------------------------
    const periodo_backup = `${Date.now()}`;

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

        NULLIF(fs.renta, '')::numeric,
        NULLIF(fs.fecha::text,'')::date,
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

        NULLIF(fs.tarifa_venta, '')::numeric,
        NULLIF(fs.comision_neta, '')::numeric,
        NULLIF(fs.punto_equilibrio, '')::numeric,

        fs.source_file
      FROM staging.siapp_full fs
      WHERE NULLIF(fs.fecha::text,'')::date IS NOT NULL
        AND ($2::int IS NULL OR EXTRACT(YEAR FROM NULLIF(fs.fecha::text,'')::date)::int = $2)
        AND ($3::int IS NULL OR EXTRACT(MONTH FROM NULLIF(fs.fecha::text,'')::date)::int = $3)
      `,
      [periodo_backup, periodFilter?.year ?? null, periodFilter?.month ?? null]
    );

    // ----------------------------------------------------
    // 4) Si mode=rebuild => borrar destino SOLO para meses a insertar
    //    Si mode=merge   => NO borrar nada
    // ----------------------------------------------------
    if (safeMode === "rebuild") {
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
    }

    // ----------------------------------------------------
    // 5) Insert / Upsert desde staging → full_sales
    //    - Rebuild: INSERT normal
    //    - Merge: UPSERT por OT (requiere ux_full_sales_ot)
    // ----------------------------------------------------
    let total_insertados = 0;
    let total_actualizados = 0;

    if (safeMode === "merge") {
      // Nota: ON CONFLICT usa tu índice único por (ot) con el WHERE.
      // Como staging garantiza ot no vacío, no hay problema.
      const upsertQ = await client.query(
        `
        WITH upserted AS (
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
            raw_json, source_file,
            updated_at
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

            cantserv,                      -- VARCHAR

            tipored,
            division,
            area,
            zona,
            poblacion,
            d_distrito,

            NULLIF(renta,'')::numeric,
            NULLIF(fecha::text,'')::date,
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
            COALESCE(source_file, $1),
            NOW()
          FROM staging.siapp_full
          WHERE NULLIF(fecha::text,'')::date IS NOT NULL
            AND ($2::int IS NULL OR EXTRACT(YEAR FROM NULLIF(fecha::text,'')::date)::int = $2)
            AND ($3::int IS NULL OR EXTRACT(MONTH FROM NULLIF(fecha::text,'')::date)::int = $3)

          ON CONFLICT (ot) WHERE ot IS NOT NULL AND BTRIM(ot) <> ''
          DO UPDATE SET
            period_year        = EXCLUDED.period_year,
            period_month       = EXCLUDED.period_month,

            estado_liquidacion = EXCLUDED.estado_liquidacion,
            linea_negocio      = EXCLUDED.linea_negocio,
            cuenta             = EXCLUDED.cuenta,

            idasesor           = EXCLUDED.idasesor,
            nombreasesor       = EXCLUDED.nombreasesor,
            cantserv           = EXCLUDED.cantserv,
            tipored            = EXCLUDED.tipored,
            division           = EXCLUDED.division,
            area               = EXCLUDED.area,
            zona               = EXCLUDED.zona,
            poblacion          = EXCLUDED.poblacion,
            d_distrito         = EXCLUDED.d_distrito,

            renta              = EXCLUDED.renta,
            fecha              = EXCLUDED.fecha,
            venta              = EXCLUDED.venta,

            tipo_registro      = EXCLUDED.tipo_registro,
            estrato            = EXCLUDED.estrato,
            paquete_pvd        = EXCLUDED.paquete_pvd,
            mintic             = EXCLUDED.mintic,
            tipo_prodcuto      = EXCLUDED.tipo_prodcuto,
            ventaconvergente   = EXCLUDED.ventaconvergente,
            venta_instale_dth  = EXCLUDED.venta_instale_dth,
            sac_final          = EXCLUDED.sac_final,

            cedula_vendedor    = EXCLUDED.cedula_vendedor,
            nombre_vendedor    = EXCLUDED.nombre_vendedor,
            modalidad_venta    = EXCLUDED.modalidad_venta,
            tipo_vendedor      = EXCLUDED.tipo_vendedor,
            tipo_red_comercial = EXCLUDED.tipo_red_comercial,

            nombre_regional    = EXCLUDED.nombre_regional,
            nombre_comercial   = EXCLUDED.nombre_comercial,
            nombre_lider       = EXCLUDED.nombre_lider,
            retencion_control  = EXCLUDED.retencion_control,
            observ_retencion   = EXCLUDED.observ_retencion,
            tipo_contrato      = EXCLUDED.tipo_contrato,

            tarifa_venta       = EXCLUDED.tarifa_venta,
            comision_neta      = EXCLUDED.comision_neta,
            punto_equilibrio   = EXCLUDED.punto_equilibrio,

            raw_json           = EXCLUDED.raw_json,
            source_file        = EXCLUDED.source_file,
            updated_at         = NOW()

          RETURNING (xmax = 0) AS inserted
        )
        SELECT
          SUM(CASE WHEN inserted THEN 1 ELSE 0 END)::bigint AS inserted_count,
          SUM(CASE WHEN inserted THEN 0 ELSE 1 END)::bigint AS updated_count
        FROM upserted
        `,
        [source_file, periodFilter?.year ?? null, periodFilter?.month ?? null]
      );

      total_insertados = Number(upsertQ.rows[0]?.inserted_count || 0);
      total_actualizados = Number(upsertQ.rows[0]?.updated_count || 0);
    } else {
      // rebuild: inserción directa
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

          cantserv,                     -- VARCHAR

          tipored,
          division,
          area,
          zona,
          poblacion,
          d_distrito,

          NULLIF(renta,'')::numeric,
          NULLIF(fecha::text,'')::date,
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

      total_insertados = insertResult.rowCount;
      total_actualizados = 0;
    }

    await client.query("COMMIT");

    return {
      ok: true,
      mode: safeMode,
      periodo_backup,
      filtro_periodo: periodFilter
        ? `${periodFilter.year}-${String(periodFilter.month).padStart(2, "0")}`
        : null,
      meses_detectados_en_staging: months,
      total_staging: totalStaging,
      filas_con_fecha_invalida_en_staging: invalidFecha,
      total_insertados,
      total_actualizados
    };
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("[PROMOTE SIAPP FULL ERROR]", err);
    throw err;
  } finally {
    client.release();
  }
}

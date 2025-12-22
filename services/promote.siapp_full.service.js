// ======================================================================
// PROMOTE SIAPP FULL — VERSIÓN FINAL 2025-12-11
// Correcciones:
//   - cantserv se conserva como VARCHAR (NO numeric)
//   - casteo correcto de campos numeric reales
//   - backup liviano y ordenado
//   - periodo_backup único
// ======================================================================

import pool from "../config/database.js";

export async function promoteSiappFull({ period_year, period_month, source_file = null }) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // ----------------------------------------------------
    // 1. Validar que staging tiene datos
    // ----------------------------------------------------
    const check = await client.query(`SELECT COUNT(*) AS total FROM staging.siapp_full`);
    if (Number(check.rows[0].total) === 0) {
      throw new Error("No hay datos en staging.siapp_full");
    }

    const periodo_comercial = `${period_year}-${String(period_month).padStart(2, "0")}`;
    const periodo_backup = `${periodo_comercial}-${Date.now()}`;

    // =====================================================================
    // 2. BACKUP (SOLO COLUMNAS NECESARIAS + CAST CORRECTOS)
    // =====================================================================
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
  $1,
  $2,

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
  fs.fecha,
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

      `,
      [periodo_comercial, periodo_backup]
    );

    console.log(`✔ Se creó backup exitoso: ${periodo_backup}`);

    // ----------------------------------------------------
    // 3. Limpiar tabla destino
    // ----------------------------------------------------
    await client.query(`TRUNCATE siapp.full_sales RESTART IDENTITY`);

    // ----------------------------------------------------
    // 4. Insertar desde staging → full_sales
    // ----------------------------------------------------
    await client.query(
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
        $1, $2,

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
        fecha,
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
        COALESCE(source_file, $3)
      FROM staging.siapp_full
      `,
      [period_year, period_month, source_file]
    );

    await client.query("COMMIT");

    return {
      ok: true,
      periodo_comercial,
      periodo_backup,
      total_insertados: Number(check.rows[0].total)
    };

  } catch (err) {
    await client.query("ROLLBACK");
    console.error("[PROMOTE SIAPP FULL ERROR]", err);
    throw err;

  } finally {
    client.release();
  }
}

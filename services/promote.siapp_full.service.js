// services/promote.siapp_full.service.js
import pool from "../config/database.js";
import crypto from "crypto";

/** Hash SHA-256 seguro para detectar duplicados */
function hashRow(row) {
  return crypto
    .createHash("sha256")
    .update(JSON.stringify(row))
    .digest("hex");
}

export async function promoteSiappFull({ period_year, period_month, source_file = null }) {
  if (!period_year || !period_month) {
    throw new Error("Falta period_year o period_month");
  }

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    /***********************************************************
     * 1. LEER TODA LA DATA DEL STAGING (archivo importado)
     ***********************************************************/
    const { rows } = await client.query(`
      SELECT *
      FROM staging.siapp_full
      ORDER BY id ASC
    `);

    if (!rows.length) {
      throw new Error("No hay datos en staging.siapp_full");
    }

    console.log(`[PROMOTE] ${rows.length} filas encontradas en staging`);

    /***********************************************************
     * 2. LIMPIAR COMPLETAMENTE LA TABLA FULL (IMPORTANTE)
     *
     * Esto asegura que:
     * ✔ Nunca queden datos de cargas anteriores
     * ✔ full_sales SIEMPRE refleje exactamente el archivo SIAPP
     ***********************************************************/
    await client.query(`
      TRUNCATE siapp.full_sales RESTART IDENTITY
    `);

    let inserted = 0;

    /***********************************************************
     * 3. INSERTAR CADA FILA CON EL PERÍODO FIJO (Y,M)
     ***********************************************************/
    for (const r of rows) {
      // raw_json sin campos internos
      const raw = { ...r };
      delete raw.id;
      delete raw.imported_at;

      const rowHash = hashRow(raw);

      await client.query(`
        INSERT INTO siapp.full_sales (
          period_year, period_month,

          estado_liquidacion, linea_negocio, cuenta, ot,
          idasesor, nombreasesor, cantserv, tipored,
          division, area, zona, poblacion, d_distrito,
          renta, fecha, venta, tipo_registro, estrato,
          paquete_pvd, mintic, tipo_prodcuto, ventaconvergente,
          venta_instale_dth, sac_final, cedula_vendedor, nombre_vendedor,
          modalidad_venta, tipo_vendedor, tipo_red_comercial,
          nombre_regional, nombre_comercial, nombre_lider,
          retencion_control, observ_retencion, tipo_contrato,
          tarifa_venta, comision_neta, punto_equilibrio,

          raw_json, raw_hash, source_file_name, validated
        )
        VALUES (
          $1,$2,
          $3,$4,$5,$6,$7,$8,$9,$10,
          $11,$12,$13,$14,$15,$16,
          $17,$18,$19,$20,$21,$22,$23,$24,$25,$26,
          $27,$28,$29,$30,$31,$32,$33,$34,$35,$36,
          $37,$38,$39,$40,$41,$42,$43,$44
        )
      `,
      [
        period_year,
        period_month,

        r.estado_liquidacion, r.linea_negocio, r.cuenta, r.ot,
        r.idasesor, r.nombreasesor, r.cantserv, r.tipored,
        r.division, r.area, r.zona, r.poblacion, r.d_distrito,
        r.renta, r.fecha, r.venta, r.tipo_registro, r.estrato,
        r.paquete_pvd, r.mintic, r.tipo_prodcuto, r.ventaconvergente,
        r.venta_instale_dth, r.sac_final, r.cedula_vendedor, r.nombre_vendedor,
        r.modalidad_venta, r.tipo_vendedor, r.tipo_red_comercial,
        r.nombre_regional, r.nombre_comercial, r.nombre_lider,
        r.retencion_control, r.observ_retencion, r.tipo_contrato,
        r.tarifa_venta, r.comision_neta, r.punto_equilibrio,

        raw,
        rowHash,
        source_file,
        true
      ]);

      inserted++;
    }

    /***********************************************************
     * 4. LIMPIAR STAGING DESPUÉS DEL PROMOTE
     ***********************************************************/
    await client.query(`TRUNCATE staging.siapp_full`);

    await client.query("COMMIT");

    return {
      ok: true,
      message: "Promoción SIAPP FULL completada correctamente",
      inserted,
      total_from_file: rows.length,
      period_year,
      period_month
    };

  } catch (e) {
    await client.query("ROLLBACK");
    console.error("[PROMOTE SIAPP FULL ERROR]", e);
    throw e;

  } finally {
    client.release();
  }
}

// services/promote.presupuesto_jerarquia.service.js
// ======================================================================
//  PROMOTE — PRESUPUESTO JERARQUÍA — 2025-12 (alineado a tu esquema real)
//  staging.presupuesto_jerarquia  -> core.presupuesto_jerarquia
//
//  - NO usa period_year/period_month (tu core.presupuesto_jerarquia no los tiene)
//  - Refresca el snapshot completo: TRUNCATE core y carga desde staging
//  - Deja activo_en_periodo = true para todos los registros importados
//  - Backup opcional (si existe historico.presupuesto_jerarquia_backup)
// ======================================================================
import pool from "../config/database.js";

async function tableExists(client, schema, table) {
  const { rows } = await client.query(
    `
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = $1 AND table_name = $2
    LIMIT 1
    `,
    [schema, table]
  );
  return rows.length > 0;
}

async function backupPresupuestoJerarquia(client) {
  const exists = await tableExists(client, "historico", "presupuesto_jerarquia_backup");
  if (!exists) return { skipped: true, reason: "No existe historico.presupuesto_jerarquia_backup" };

  // Ajusta columnas si tu tabla histórica tiene otras.
  await client.query(`
    INSERT INTO historico.presupuesto_jerarquia_backup (
      cargo_raw,
      cedula,
      nombre_raw,
      distrito_raw,
      regional_raw,
      fecha_inicio,
      fecha_fin,
      presupuesto_raw,
      capacidad_raw,
      telefono_raw,
      correo_raw,
      ejecutado_raw,
      cierre_raw,
      jerarquia_raw,
      contratado_raw,
      activo_en_periodo,
      created_at,
      backed_up_at
    )
    SELECT
      cargo_raw,
      cedula,
      nombre_raw,
      distrito_raw,
      regional_raw,
      fecha_inicio,
      fecha_fin,
      presupuesto_raw,
      capacidad_raw,
      telefono_raw,
      correo_raw,
      ejecutado_raw,
      cierre_raw,
      jerarquia_raw,
      contratado_raw,
      activo_en_periodo,
      created_at,
      now()
    FROM core.presupuesto_jerarquia
  `);

  return { skipped: false };
}

export async function promotePresupuestoJerarquia({ do_backup = false } = {}) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // 1) Verificar staging con datos
    const { rows: stCount } = await client.query(
      `SELECT COUNT(*)::int AS total FROM staging.presupuesto_jerarquia`
    );
    const totalStaging = Number(stCount[0]?.total || 0);

    if (totalStaging <= 0) {
      await client.query("ROLLBACK");
      return { ok: false, message: "staging.presupuesto_jerarquia está vacío", total_staging: 0 };
    }

    // 2) Backup opcional
    let backup = { skipped: true, reason: "backup deshabilitado" };
    if (do_backup) {
      backup = await backupPresupuestoJerarquia(client);
    }

    // 3) Refrescar snapshot completo
    // Nota: como core.presupuesto_jerarquia es “foto” del Excel,
    //       truncamos e insertamos todo lo del staging.
    await client.query(`TRUNCATE TABLE core.presupuesto_jerarquia RESTART IDENTITY`);

    // staging.presupuesto_jerarquia (según tu imports.service.js) trae columnas:
    // cedula, nivel, nombre, cargo, distrito, regional, presupuesto, telefono, correo, capacidad
    // Las mapeamos a core.presupuesto_jerarquia raws + jerarquia_raw.
    const ins = await client.query(`
      INSERT INTO core.presupuesto_jerarquia (
        jerarquia_raw,
        cargo_raw,
        cedula,
        nombre_raw,
        distrito_raw,
        regional_raw,
        presupuesto_raw,
        capacidad_raw,
        telefono_raw,
        correo_raw,
        -- estos 2 pueden quedar null si no los tienes en staging
        ejecutado_raw,
        cierre_raw,
        -- contratado_raw viene vacío en tu Excel actual
        contratado_raw,
        activo_en_periodo
      )
      SELECT
        NULLIF(TRIM(nivel), '')                 AS jerarquia_raw,
        NULLIF(TRIM(cargo), '')                 AS cargo_raw,
        NULLIF(TRIM(cedula), '')                AS cedula,
        NULLIF(TRIM(nombre), '')                AS nombre_raw,
        NULLIF(TRIM(distrito), '')              AS distrito_raw,
        NULLIF(TRIM(regional), '')              AS regional_raw,
        presupuesto                              AS presupuesto_raw,
        capacidad                                AS capacidad_raw,
        NULLIF(TRIM(telefono), '')              AS telefono_raw,
        NULLIF(TRIM(correo), '')                AS correo_raw,
        NULL                                     AS ejecutado_raw,
        NULL                                     AS cierre_raw,
        NULL                                     AS contratado_raw,
        true                                     AS activo_en_periodo
      FROM staging.presupuesto_jerarquia
      ORDER BY cedula ASC
      RETURNING 1
    `);

    await client.query("COMMIT");

    return {
      ok: true,
      message: "Promote Presupuesto Jerarquía completado (snapshot staging -> core)",
      total_staging: totalStaging,
      inserted: ins.rowCount,
      backup: do_backup ? backup : { skipped: true, reason: "backup deshabilitado" }
    };
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("[PROMOTE PJ ERROR]", err);
    throw err;
  } finally {
    client.release();
  }
}

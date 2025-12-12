// ======================================================================
//  PROMOTE — PRESUPUESTO JERARQUÍA (VERSIÓN FINAL 2025-12-10)
// ======================================================================
import pool from "../config/database.js";

// ----------------------------------------------------------
// UTILIDADES
// ----------------------------------------------------------
function norm(s) {
  if (!s) return "";
  return String(s)
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
}

// ======================================================================
//  DISTRITOS CANONIZADOS + ALIAS  (NO MODIFICADOS)
// ======================================================================
const DISTRICT_STD = {
  "GERENCIA COMERCIAL": "GERENCIA COMERCIAL",
  "DIRECTOR OPERATIVO FIJO NACIONAL": "DIRECTOR OPERATIVO FIJO NACIONAL",




  "DIRECTOR META": "META",
  "DIRECTOR SANTANDER": "SANTANDER",
  "DIRECTOR NORTE SANTANDER, TOLIMA Y HUILA": "NORTE SANTANDER",
  "TOLIMA Y HUILA": "TOLIMA-HUILA",




  "DIRECTOR MEDELLIN OCCIDENTAL": "MEDELLIN OCCIDENTAL",
  "DIRECTOR MEDELLIN NOROCCIDENTAL": "MEDELLIN NOROCCIDENTE",
  "DIRECTOR CALI Y CAUCA": "CALI-CAUCA",




  "BELLO METROPOLITANO": "BELLO METROPOLITANO",
  "BELLO NORTE": "BELLO NORTE",
  "MEDELLIN OCCIDENTAL 4": "MEDELLIN OCCIDENTAL 4",
  "MEDELLIN NOROCCIDENTE 1": "MEDELLIN NOROCCIDENTE 1",
  "MEDELLIN NOROCCIDENTE 2": "MEDELLIN NOROCCIDENTE 2",




  "CAUCA DISTRITO 1": "CAUCA 1",
  "ALFONSO LOPEZ": "ALFONSO LOPEZ",
  "CHIPICHAPE": "CHIPICHAPE",
  "COMUNEROS": "COMUNEROS",
  "FLORALIA": "FLORALIA",
  "EL JARDIN": "EL JARDIN",
  "YUMBO": "YUMBO",




  "HUILA DISTRITO 6": "HUILA 6",
  "PITALITO DISTRITO 1": "PITALITO",




  "TOLIMA DISTRITO 2.1": "TOLIMA 2.1",
  "TOLIMA DISTRITO 2.2": "TOLIMA 2.2",
  "TOLIMA DISTRITO 3": "TOLIMA 3",








  "CUCUTA 1": "CUCUTA 1",
  "CUCUTA 3": "CUCUTA 3",
  "LOS PATIOS": "LOS PATIOS",
  "VILLA DEL ROSARIO": "VILLA DEL ROSARIO",




  "LLANOS 1.1": "LLANOS 1.1",
  "LLANOS 1.2": "LLANOS 1.2",
  "VILLAVICENCIO 1": "VILLAVICENCIO 1",
  "VILLAVICENCIO 2": "VILLAVICENCIO 2",
  "VILLAVICENCIO 3": "VILLAVICENCIO 3",




  "BUCARAMANGA 1": "BUCARAMANGA 1",
  "BUCARAMANGA 2": "BUCARAMANGA 2",
  "FLORIDABLANCA 1": "FLORIDABLANCA 1",
};




// --------------------------
// Aliases → distritos canonizados
// --------------------------
const DISTRICT_ALIAS = {
  "ZONA LLANOS 1.1": "LLANOS 1.1",
  "ZONA LLANOS 1.2": "LLANOS 1.2",




  "ZONA V/CIO 1": "VILLAVICENCIO 1",
  "ZONA V/CIO 2": "VILLAVICENCIO 2",
  "ZONA V/CIO 3": "VILLAVICENCIO 3",




  "ZONA VILLAVICENCIO 1": "VILLAVICENCIO 1",
  "ZONA VILLAVICENCIO 2": "VILLAVICENCIO 2",
  "ZONA VILLAVICENCIO 3": "VILLAVICENCIO 3",




  "ZONA CAUCA 1": "CAUCA 1",
  "ZONA CAUCA 4": "CAUCA 1",




  "ZONA CHIPICHAPE": "CHIPICHAPE",
  "ZONA COMUNEROS": "COMUNEROS",
  "ZONA YUMBO": "YUMBO",
  "ZONA ALFONSO LOPEZ": "ALFONSO LOPEZ",




  "ZONA HUILA 6": "HUILA DISTRITO 6",
  "ZONA GARZON": "HUILA DISTRITO 6",
  "HUILA 6": "HUILA DISTRITO 6",




  "ZONA TOLIMA 2.1": "TOLIMA DISTRITO 2.1",
  "ZONA TOLIMA 2.2": "TOLIMA DISTRITO 2.2",
  "ZONA TOLIMA 3": "TOLIMA DISTRITO 3",




  "ZONA CUCUTA 1": "CUCUTA 1",
  "ZONA CUCUTA 3": "CUCUTA 3",
  "ZONA PATIOS": "LOS PATIOS",
  "ZONA VILLA DEL ROSARIO": "VILLA DEL ROSARIO",




  "ZONA MEDELLIN - BELLO METROP": "BELLO METROPOLITANO",
  "ZONA MEDELLIN - BELLO NORTE": "BELLO NORTE",
  "ZONA MEDELLIN - NOROCC 1": "MEDELLIN NOROCCIDENTE 1",
  "ZONA MEDELLIN - NOROCC 2": "MEDELLIN NOROCCIDENTE 2",
  "ZONA MEDELLIN - OCC 2": "MEDELLIN OCCIDENTAL 4",
  "ZONA MEDELLIN - OCC 4": "MEDELLIN OCCIDENTAL 4",
  "MEDELLIN OCCIDENTAL": "MEDELLIN OCCIDENTAL 4",




  "ZONA B/MANGA 1": "BUCARAMANGA 1",
  "ZONA B/MANGA 2": "BUCARAMANGA 2",
  "ZONA B/MANGA 3": "BUCARAMANGA 2",




  "ZONA FLORIDABLANCA 1": "FLORIDABLANCA 1",
  "ZONA FLORIDABLANCA 2": "FLORIDABLANCA 1",




  "ZONA FLORALIA": "FLORALIA",
  "ZONA EL JARDIN": "EL JARDIN",
    // --- NUEVOS PARA B/MANGA ---
  "B/MANGA 1": "BUCARAMANGA 1",
  "B/MANGA 2": "BUCARAMANGA 2",
  "B/MANGA 3": "BUCARAMANGA 2",




  // --- CAUCA ---
  "CAUCA 4": "CAUCA 1",




  // --- FLORIDABLANCA ---
  "FLORIDABLANCA 2": "FLORIDABLANCA 1",




  // --- GARZON (HUILA) ---
  "GARZON": "HUILA DISTRITO 6",
  "ZONA GARZON": "HUILA DISTRITO 6",




  // --- MEDELLIN ---
  "MEDELLIN - NOROCC 1": "MEDELLIN NOROCCIDENTE 1",
  "MEDELLIN - NOROCC 2": "MEDELLIN NOROCCIDENTE 2",
  "MEDELLIN - NOROCC 4": "MEDELLIN NOROCCIDENTE 1",
  "MEDELLIN - OCC 2": "MEDELLIN OCCIDENTAL 4",
  "MEDELLIN - OCC 4": "MEDELLIN OCCIDENTAL 4",




  // --- OCAÑA ---
  "OCANA": "CUCUTA 3",
  "ZONA OCANA": "CUCUTA 3",








  // --- CALI / VALLE GRANDE ---
  "VALLE GRANDE": "CHIPICHAPE",
    // ---- CAUCA ----
  "CAUCA 1": "CAUCA DISTRITO 1",
  "ZONA CAUCA 1": "CAUCA DISTRITO 1",
  "CAUCA 4": "CAUCA DISTRITO 1",




  // ---- HUILA ----
  "ZONA HUILA 6": "HUILA DISTRITO 6",




  // ---- MEDELLÍN NOROCCIDENTE ----
  "MEDELLIN NOROCCIDENTE 1": "MEDELLIN NOROCCIDENTE 1",
  "ZONA MEDELLIN NOROCC 1": "MEDELLIN NOROCCIDENTE 1",
  "MEDELLIN - NOROCC 1": "MEDELLIN NOROCCIDENTE 1",




  "MEDELLIN NOROCCIDENTE 2": "MEDELLIN NOROCCIDENTE 2",
  "ZONA MEDELLIN NOROCC 2": "MEDELLIN NOROCCIDENTE 2",
  "MEDELLIN - NOROCC 2": "MEDELLIN NOROCCIDENTE 2",




  // ---- MEDELLIN OCCIDENTAL ----
  "MEDELLIN OCCIDENTAL 4": "MEDELLIN OCCIDENTAL 4",
  "MEDELLIN - OCC 4": "MEDELLIN OCCIDENTAL 4",
  "ZONA MEDELLIN OCC 4": "MEDELLIN OCCIDENTAL 4",
  "MEDELLIN - OCC 2": "MEDELLIN OCCIDENTAL 4", // ajuste por inexistencia




  // ---- TOLIMA ----








  "ZONA TOLIMA 3": "TOLIMA DISTRITO 3",




  // ---- CALI / VALLE ----
  "VALLE GRANDE": "CHIPICHAPE",
  "ZONA VALLE GRANDE": "CHIPICHAPE",
    // ---- CAUCA ----
  "CAUCA 1": "CAUCA DISTRITO 1",
  "ZONA CAUCA 1": "CAUCA DISTRITO 1",
  "CAUCA 4": "CAUCA DISTRITO 1",








  // ---- MEDELLIN OCCIDENTAL ----
  "MEDELLIN OCCIDENTAL": "MEDELLIN OCCIDENTAL 4",
  "MEDELLIN OCCIDENTAL 4": "MEDELLIN OCCIDENTAL 4",
  "ZONA MEDELLIN OCC 4": "MEDELLIN OCCIDENTAL 4",
  "MEDELLIN - OCC 4": "MEDELLIN OCCIDENTAL 4",
  "MEDELLIN": "MEDELLIN OCCIDENTAL 4",




  // ---- MED NOROCCIDENTE ----
  "MEDELLIN NOROCCIDENTE 1": "MEDELLIN NOROCCIDENTE 1",
  "ZONA MEDELLIN NOROCC 1": "MEDELLIN NOROCCIDENTE 1",
  "MEDELLIN - NOROCC 1": "MEDELLIN NOROCCIDENTE 1",




  "MEDELLIN NOROCCIDENTE 2": "MEDELLIN NOROCCIDENTE 2",
  "ZONA MEDELLIN NOROCC 2": "MEDELLIN NOROCCIDENTE 2",
  "MEDELLIN - NOROCC 2": "MEDELLIN NOROCCIDENTE 2",




  // ---- TOLIMA 2.1 ----
  "ZONA TOLIMA 2.1 (ESPINAL)": "TOLIMA 2.1",
  "ZONA TOLIMA 2.1 ESPINAL": "TOLIMA DISTRITO 2.1",
  "ZONA TOLIMA 2.1 (GUAMO)": "TOLIMA DISTRITO 2.1",
  "ZONA TOLIMA 2.1 GUAMO": "TOLIMA DISTRITO 2.1",




  // ---- TOLIMA 2.2 ----
  "ZONA TOLIMA 2.2 (FLANDES)": "TOLIMA DISTRITO 2.2",
  "ZONA TOLIMA 2.2 FLANDES": "TOLIMA DISTRITO 2.2",
  "ZONA TOLIMA 2.2 (MELGAR)": "TOLIMA DISTRITO 2.2",
  "ZONA TOLIMA 2.2 MELGAR": "TOLIMA DISTRITO 2.2",




  // ---- TOLIMA 3 ----
  "ZONA TOLIMA 3 (IBAGUE)": "TOLIMA 3",
  "ZONA TOLIMA 3 IBAGUE": "TOLIMA DISTRITO 3",
};



function canonizeDistrict(raw) {
  if (!raw) return "";
  let t = norm(raw);

  t = t.replace(/^ZONA\s+/, "").trim();

  if (DISTRICT_ALIAS[t]) return norm(DISTRICT_ALIAS[t]);
  if (DISTRICT_STD[t]) return norm(DISTRICT_STD[t]);

  t = t.replace(/V\/CIO/g, "VILLAVICENCIO");

  for (const std of Object.values(DISTRICT_STD)) {
    const nstd = norm(std);
    if (t.includes(nstd)) return nstd;
    if (nstd.includes(t)) return nstd;
  }

  return t;
}

// ======================================================================
//  JERARQUÍA
// ======================================================================
function resolveJerarquia(j) {
  const raw = norm(j.jerarquia_raw || j.jerarquia || j.cargo_raw || "");
  if (raw.includes("GEREN")) return "GERENCIA";
  if (raw.includes("DIREC")) return "DIRECCION";
  if (raw.includes("COOR"))  return "COORDINACION";
  if (raw.includes("ASES"))  return "ASESORIA";
  return null;
}

// ======================================================================
//  ORG UNITS
// ======================================================================
async function getOrCreateOrgUnit(name, type, parent = null) {
  const nm = norm(name);

  const exists = await pool.query(
    `SELECT id FROM core.org_units WHERE name=$1 AND unit_type=$2 LIMIT 1`,
    [nm, type]
  );
  if (exists.rows.length) return exists.rows[0].id;

  const ins = await pool.query(
    `INSERT INTO core.org_units (name,unit_type,parent_id)
     VALUES ($1,$2,$3)
     RETURNING id`,
    [nm, type, parent]
  );

  return ins.rows[0].id;
}

// ======================================================================
// USERS — CORREGIDO PARA NO SOBRESCRIBIR CONTRASEÑAS
// ======================================================================
async function getOrCreateUser(row, orgUnit, inheritedRegional = null, coordinatorId = null) {
  const phone = row.telefono_raw || "0000";
  let email = row.correo_raw;

  if (!email || norm(email) === "NO CONTRATADO") {
    email = `${row.cedula}@movilco.com`;
  }

  const exists = await pool.query(
    `SELECT id, password_hash FROM core.users WHERE document_id=$1 LIMIT 1`,
    [row.cedula]
  );

  const passHash =
    exists.rows.length && exists.rows[0].password_hash
      ? exists.rows[0].password_hash
      : `$2b$10$DEFAULT_${phone}_${row.cedula}`;

  const payload = [
    row.nombre_raw || "SIN NOMBRE",
    email,
    phone,
    row.role,
    passHash,
    true,
    orgUnit,
    canonizeDistrict(row.distrito_raw),
    row.cedula,
    row.fecha_inicio,
    row.fecha_fin,
    inheritedRegional || row.regional_raw || null,
    coordinatorId,
    row.cargo_raw,
    row.capacidad_raw,
    row.role,
    row.presupuesto_raw,
    row.ejecutado_raw,
    row.cierre_raw
  ];

  if (exists.rows.length) {
    const id = exists.rows[0].id;

    await pool.query(
      `UPDATE core.users
       SET name=$1,email=$2,phone=$3,role=$4,password_hash=$5,active=$6,
           org_unit_id=$7,district=$8,document_id=$9,contract_start=$10,
           contract_end=$11,regional=$12,coordinator_id=$13,cargo=$14,
           capacity=$15,jerarquia=$16,presupuesto=$17,ejecutado=$18,
           cierre_porcentaje=$19,updated_at=NOW()
       WHERE id=$20`,
      [...payload, id]
    );

    return id;
  }

  const inserted = await pool.query(
    `INSERT INTO core.users
     (name,email,phone,role,password_hash,active,
      org_unit_id,district,document_id,contract_start,contract_end,
      regional,coordinator_id,cargo,capacity,jerarquia,
      presupuesto,ejecutado,cierre_porcentaje)
     VALUES
     ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
     RETURNING id`,
    payload
  );

  return inserted.rows[0].id;
}

// ======================================================================
// BACKUP HISTÓRICO CORRECTO
// ======================================================================
async function backupPresupuesto(periodo) {
  await pool.query(
    `
      INSERT INTO historico.presupuesto_jerarquia_backup (
        id, jerarquia_raw, cargo_raw, cedula, nombre_raw, contratado_raw,
        distrito_raw, regional_raw, fecha_inicio, fecha_fin,
        presupuesto_raw, ejecutado_raw, cierre_raw, capacidad_raw,
        telefono_raw, correo_raw, periodo
      )
      SELECT
        id, jerarquia_raw, cargo_raw, cedula, nombre_raw, contratado_raw,
        distrito_raw, regional_raw, fecha_inicio, fecha_fin,
        presupuesto_raw, ejecutado_raw, cierre_raw, capacidad_raw,
        telefono_raw, correo_raw,
        $1 AS periodo
      FROM core.presupuesto_jerarquia;
    `,
    [periodo]
  );

  console.log(`[BACKUP PJ] Backup histórico creado para periodo ${periodo}`);
}

// ======================================================================
//  PROMOTE PRINCIPAL — VERSIÓN FINAL
// ======================================================================
export async function promotePresupuestoJerarquia() {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // ============================================================
    // 0) PERIODO ACTUAL
    // ============================================================
    const periodo = new Date().toISOString().slice(0, 7);
    const [yy, mm] = periodo.split("-").map(Number);

    // ============================================================
    // 1) Cargar datos actuales del archivo (staging)
    // ============================================================
    const { rows: staging } = await client.query(`
      SELECT *
      FROM staging.presupuesto_jerarquia
      ORDER BY cedula ASC
    `);

    if (!staging.length)
      return { ok: false, message: "staging.presupuesto_jerarquia está vacío" };

    // ============================================================
    // 2) Marcar TODOS los usuarios como inactivos en este periodo
    // ============================================================
    await client.query(`
      UPDATE core.presupuesto_jerarquia
      SET activo_en_periodo = false
      WHERE period_year = $1 AND period_month = $2
    `, [yy, mm]);

    // ============================================================
    // 3) Procesar staging → insertar o actualizar registros del periodo
    // ============================================================
    for (const r of staging) {
      await client.query(`
        INSERT INTO core.presupuesto_jerarquia (
          cedula, nombre_raw, cargo_raw, distrito_raw, regional_raw,
          presupuesto_raw, capacidad_raw, telefono_raw, correo_raw,
          period_year, period_month, activo_en_periodo
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,true)
        ON CONFLICT (cedula, period_year, period_month)
        DO UPDATE SET
          nombre_raw = EXCLUDED.nombre_raw,
          cargo_raw = EXCLUDED.cargo_raw,
          distrito_raw = EXCLUDED.distrito_raw,
          regional_raw = EXCLUDED.regional_raw,
          presupuesto_raw = EXCLUDED.presupuesto_raw,
          capacidad_raw = EXCLUDED.capacidad_raw,
          telefono_raw = EXCLUDED.telefono_raw,
          correo_raw = EXCLUDED.correo_raw,
          activo_en_periodo = true
      `, [
        r.cedula,
        r.nombre,
        r.cargo,
        r.distrito,
        r.regional,
        r.presupuesto,
        r.capacidad,
        r.telefono,
        r.correo,
        yy,
        mm
      ]);
    }

    // ============================================================
    // 4) Cargar nuevamente los registros para reconstrucción jerárquica
    // ============================================================
    const { rows } = await client.query(`
      SELECT *
      FROM core.presupuesto_jerarquia
      WHERE period_year = $1 AND period_month = $2 AND activo_en_periodo = true
      ORDER BY id ASC
    `, [yy, mm]);

    // ============================================================
    // 5) Restaurar jerarquía actual (MISMO CÓDIGO QUE YA TENÍAS)
    // ============================================================
    await client.query(`
      UPDATE core.users
      SET org_unit_id = NULL,
          coordinator_id = NULL,
          jerarquia = NULL,
          presupuesto = NULL,
          ejecutado = NULL,
          cierre_porcentaje = NULL,
          capacidad = NULL,
          updated_at = NOW()
    `);

    const ger = [], dir = [], coord = [], ases = [];

    for (const r of rows) {
      r.role = resolveJerarquia(r);
      r.distrito_raw = canonizeDistrict(r.distrito_raw);

      if (!r.role) continue;

      if (r.role === "GERENCIA") ger.push(r);
      else if (r.role === "DIRECCION") dir.push(r);
      else if (r.role === "COORDINACION") coord.push(r);
      else ases.push(r);
    }

    const MGER = {}, MDIR = {}, MCOOR = {};

    // GERENCIA
    for (const g of ger) {
      const unit = await getOrCreateOrgUnit("GERENCIA COMERCIAL", "GERENCIA", null);
      const user = await getOrCreateUser(g, unit);
      MGER["GERENCIA COMERCIAL"] = { unit, user };
    }

    const gerUnit = MGER["GERENCIA COMERCIAL"]?.unit || null;

    // DIRECCIÓN
    for (const d of dir) {
      const unit = await getOrCreateOrgUnit(d.distrito_raw, "DIRECCION", gerUnit);
      const user = await getOrCreateUser(d, unit, d.regional_raw);
      MDIR[d.distrito_raw] = { unit, user };
    }

    // COORDINACIÓN
    for (const c of coord) {
      const parent = MDIR[c.distrito_raw];
      const parentUnit = parent?.unit || null;
      const parentUser = parent?.user || null;

      const unit = await getOrCreateOrgUnit(c.distrito_raw, "COORDINACION", parentUnit);
      const user = await getOrCreateUser(c, unit, c.regional_raw, parentUser);

      MCOOR[c.distrito_raw] = { unit, user, regional: c.regional_raw };
    }

    // ASESORÍA
    for (const a of ases) {
      const d = canonizeDistrict(a.distrito_raw);
      const parent = MCOOR[d];

      if (!parent) {
        console.warn(`⚠ Asesor sin coordinador: ${a.nombre_raw}`);
        continue;
      }

      await getOrCreateUser(a, parent.unit, parent.regional, parent.user);
    }

    await client.query("COMMIT");

    return {
      ok: true,
      message: "Promote Presupuesto Jerarquía completado correctamente",
      periodo,
      activos_en_periodo: rows.length
    };

  } catch (err) {
    await client.query("ROLLBACK");
    console.error("[ERROR PROMOTE PJ]", err);
    throw err;

  } finally {
    client.release();
  }
}


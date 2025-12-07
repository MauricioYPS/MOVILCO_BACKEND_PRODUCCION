// ======================================================================
//  PROMOTE — PRESUPUESTO JERARQUÍA (VERSIÓN CORREGIDA 2025-12-06)
// ======================================================================
import pool from "../config/database.js";

// ----------------------------------------------------------
// UTILIDADES BÁSICAS
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
// 1) DISTRITOS CANONIZADOS Y ALIAS
// ======================================================================

// --- (Se mantiene exactamente tu tabla DISTRICT_STD) ---
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

// ----------------------------------------------------------
// FUNCIÓN CANONIZADORA DEFINITIVA
// ----------------------------------------------------------
function canonizeDistrict(raw) {
  if (!raw) return "";

  let t = norm(raw);

  // 1. eliminar prefijo "ZONA"
  t = t.replace(/^ZONA\s+/, "").trim();

  // 2. si es alias exacto
  if (DISTRICT_ALIAS[t]) {
    return norm(DISTRICT_ALIAS[t]);
  }

  // 3. si es estándar exacto
  if (DISTRICT_STD[t]) {
    return norm(DISTRICT_STD[t]);
  }

  // 4. reparaciones suaves
  t = t.replace(/V\/CIO/g, "VILLAVICENCIO");

  // 5. búsqueda aproximada
  for (const std of Object.values(DISTRICT_STD)) {
    const nstd = norm(std);
    if (t.includes(nstd)) return nstd;
    if (nstd.includes(t)) return nstd;
  }

  return t; // fallback
}

// ======================================================================
// 2) RESOLVER JERARQUÍA
// ======================================================================
function resolveJerarquia(row) {
  const raw = norm(row.jerarquia_raw || row.jerarquia || row.cargo_raw || "");

  if (raw.includes("GEREN")) return "GERENCIA";
  if (raw.includes("DIREC")) return "DIRECCION";
  if (raw.includes("COOR")) return "COORDINACION";
  if (raw.includes("ASES")) return "ASESORIA";

  return null;
}

// ======================================================================
// 3) ORG UNITS
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
// 4) USERS
// ======================================================================
async function getOrCreateUser(row, orgUnit, inheritedRegional = null, coordinatorId = null) {
  const phone = row.telefono_raw || "0000";

  let email = row.correo_raw;
  if (!email || norm(email) === "NO CONTRATADO") {
    email = `${row.cedula}@movilco.com`;
  }

  const passHash = `$2b$10$PASS_${phone}_PLACEHOLDER`;

  const exists = await pool.query(
    `SELECT id FROM core.users WHERE document_id=$1 LIMIT 1`,
    [row.cedula]
  );

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

  // UPDATE
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

  // INSERT
  const inserted = await pool.query(
    `INSERT INTO core.users
     (name,email,phone,role,password_hash,active,
      org_unit_id,district,document_id,contract_start,contract_end,
      regional,coordinator_id,cargo,capacity,jerarquia,
      presupuesto,ejecutado,cierre_porcentaje)
     VALUES
     ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
      $12,$13,$14,$15,$16,$17,$18,$19)
     RETURNING id`,
    payload
  );

  return inserted.rows[0].id;
}

// ======================================================================
// 5) PROMOTE PRINCIPAL
// ======================================================================
export async function promotePresupuestoJerarquia() {

  const { rows } = await pool.query(`SELECT * FROM core.presupuesto_jerarquia`);

  if (!rows.length)
    return { ok: false, message: "Tabla presupuesto_jerarquia vacía" };

  const ger = [], dir = [], coord = [], ases = [];

  // PRE-PROCESAR FILAS
  for (const r of rows) {
    r.role = resolveJerarquia(r);
    r.distrito_raw = canonizeDistrict(r.distrito_raw);

    if (!r.role) continue;

    if (r.role === "GERENCIA") ger.push(r);
    else if (r.role === "DIRECCION") dir.push(r);
    else if (r.role === "COORDINACION") coord.push(r);
    else if (r.role === "ASESORIA") ases.push(r);
  }

  const MGER = {};
  const MDIR = {};
  const MCOOR = {};

  // 1) GERENCIA
  for (const g of ger) {
    const unit = await getOrCreateOrgUnit("GERENCIA COMERCIAL", "GERENCIA", null);
    const user = await getOrCreateUser(g, unit);
    MGER["GERENCIA COMERCIAL"] = { user, unit };
  }

  const gerPrincipal = MGER["GERENCIA COMERCIAL"]?.unit || null;

  // 2) DIRECCIÓN
  for (const d of dir) {
    const unit = await getOrCreateOrgUnit(d.distrito_raw, "DIRECCION", gerPrincipal);
    const user = await getOrCreateUser(d, unit, d.regional_raw);
    MDIR[d.distrito_raw] = { user, unit };
  }

  // 3) COORDINACIÓN
  for (const c of coord) {
    const dirInfo = MDIR[c.distrito_raw];
    const parentUnit = dirInfo?.unit || null;
    const parentUser = dirInfo?.user || null;

    const unit = await getOrCreateOrgUnit(c.distrito_raw, "COORDINACION", parentUnit);
    const user = await getOrCreateUser(c, unit, c.regional_raw, parentUser);

    MCOOR[c.distrito_raw] = { user, unit, regional: c.regional_raw };
  }

  // 4) ASESORIA
  for (const a of ases) {
    const districtCanon = canonizeDistrict(a.distrito_raw);
    const parent = MCOOR[districtCanon];

    if (!parent) {
      console.warn(`⚠ Asesor sin coordinador: ${a.nombre_raw} (${a.distrito_raw})`);
      continue;
    }

    await getOrCreateUser(a, parent.unit, parent.regional, parent.user);
  }

  return {
    ok: true,
    message: "📌 Promote completado correctamente",
    totals: {
      gerentes: ger.length,
      directores: dir.length,
      coordinadores: coord.length,
      asesores: ases.length
    }
  };
}

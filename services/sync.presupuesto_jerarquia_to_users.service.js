// ======================================================================
//  SYNC — PRESUPUESTO JERARQUÍA -> ORG_UNITS + USERS — 2025-12
//  Lee core.presupuesto_jerarquia (periodo) y reconstruye:
//    - core.org_units (GERENCIA/DIRECCION/COORDINACION)
//    - core.users (asignación org_unit_id, coordinator_id, jerarquia, etc.)
// ======================================================================
import pool from "../config/database.js";

function norm(s) {
  if (!s) return "";
  return String(s)
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
}

function normalizePeriodInput(period) {
  if (Array.isArray(period)) return period[0];
  if (period === null || period === undefined) return null;
  return String(period).trim();
}

function parsePeriod(period) {
  const p = normalizePeriodInput(period);
  if (!p) return null;

  const m = p.match(/^(\d{4})-(\d{1,2})$/);
  if (!m) return null;

  const year = Number(m[1]);
  const month = Number(m[2]);

  if (!Number.isFinite(year) || !Number.isFinite(month)) return null;
  if (month < 1 || month > 12) return null;

  return { year, month, str: `${year}-${String(month).padStart(2, "0")}` };
}

function currentPeriod() {
  const p = new Date().toISOString().slice(0, 7);
  return parsePeriod(p);
}

// ======================================================================
//  TUS MAPAS (idénticos a tu archivo; recortados aquí por brevedad)
//  Pega completos DISTRICT_STD y DISTRICT_ALIAS desde tu archivo.
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
  // ... pega el resto tal cual lo tienes
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

function resolveJerarquia(j) {
  const raw = norm(j.jerarquia_raw || j.jerarquia || j.cargo_raw || "");
  if (raw.includes("GEREN")) return "GERENCIA";
  if (raw.includes("DIREC")) return "DIRECCION";
  if (raw.includes("COOR")) return "COORDINACION";
  if (raw.includes("ASES")) return "ASESORIA";
  return null;
}

// IMPORTANTÍSIMO: ahora todo usa client
async function getOrCreateOrgUnit(client, name, type, parent = null) {
  const nm = norm(name);

  const exists = await client.query(
    `SELECT id FROM core.org_units WHERE name=$1 AND unit_type=$2 LIMIT 1`,
    [nm, type]
  );
  if (exists.rows.length) return exists.rows[0].id;

  const ins = await client.query(
    `INSERT INTO core.org_units (name,unit_type,parent_id)
     VALUES ($1,$2,$3)
     RETURNING id`,
    [nm, type, parent]
  );

  return ins.rows[0].id;
}

async function getOrCreateUser(client, row, orgUnit, inheritedRegional = null, coordinatorId = null) {
  const phone = row.telefono_raw || "0000";
  let email = row.correo_raw;

  if (!email || norm(email) === "NO CONTRATADO") {
    email = `${row.cedula}@movilco.com`;
  }

  const exists = await client.query(
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

    await client.query(
      `UPDATE core.users
       SET name=$1,email=$2,phone=$3,role=$4,password_hash=$5,active=$6,
           org_unit_id=$7,district=$8,document_id=$9,contract_start=$10,
           contract_end=$11,regional=$12,coordinator_id=$13,cargo=$14,
           capacity=$15,jerarquia=$16,presupuesto=$17,ejecutado=$18,
           cierre_porcentaje=$19,updated_at=NOW()
       WHERE id=$20`,
      [...payload, id]
    );

    return { id, action: "updated" };
  }

  const inserted = await client.query(
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

  return { id: inserted.rows[0].id, action: "created" };
}

/**
 * @param {object} args
 * @param {string|null} args.period          "YYYY-MM"
 * @param {boolean} args.reset_all_users     Si true, hace tu UPDATE masivo a NULL (peligroso). Default false.
 *
 * Modo recomendado:
 *  - reset_all_users=false (solo actualiza/crea usuarios encontrados en el periodo)
 *  - Si necesitas EXACTAMENTE tu comportamiento anterior, ponlo en true.
 */
export async function syncPresupuestoJerarquiaToUsers({ period = null, reset_all_users = false } = {}) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const per = parsePeriod(period) || currentPeriod();
    const yy = per.year;
    const mm = per.month;

    // 1) Tomar registros activos del periodo
    const { rows } = await client.query(
      `
      SELECT *
      FROM core.presupuesto_jerarquia
      WHERE period_year=$1 AND period_month=$2 AND activo_en_periodo=true
      ORDER BY id ASC
      `,
      [yy, mm]
    );

    if (!rows.length) {
      await client.query("ROLLBACK");
      return { ok: false, message: "No hay registros activos en core.presupuesto_jerarquia para el periodo", period: per.str };
    }

    // 2) (Opcional) reset masivo como en tu archivo anterior
    if (reset_all_users) {
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
    }

    // 3) Clasificación jerárquica
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
    let usersCreated = 0;
    let usersUpdated = 0;

    // GERENCIA
    for (const g of ger) {
      const unit = await getOrCreateOrgUnit(client, "GERENCIA COMERCIAL", "GERENCIA", null);
      const u = await getOrCreateUser(client, g, unit);
      if (u.action === "created") usersCreated++;
      else usersUpdated++;
      MGER["GERENCIA COMERCIAL"] = { unit, user: u.id };
    }

    const gerUnit = MGER["GERENCIA COMERCIAL"]?.unit || null;

    // DIRECCIÓN
    for (const d of dir) {
      const unit = await getOrCreateOrgUnit(client, d.distrito_raw, "DIRECCION", gerUnit);
      const u = await getOrCreateUser(client, d, unit, d.regional_raw);
      if (u.action === "created") usersCreated++;
      else usersUpdated++;
      MDIR[d.distrito_raw] = { unit, user: u.id };
    }

    // COORDINACIÓN
    for (const c of coord) {
      const parent = MDIR[c.distrito_raw];
      const parentUnit = parent?.unit || null;
      const parentUser = parent?.user || null;

      const unit = await getOrCreateOrgUnit(client, c.distrito_raw, "COORDINACION", parentUnit);
      const u = await getOrCreateUser(client, c, unit, c.regional_raw, parentUser);
      if (u.action === "created") usersCreated++;
      else usersUpdated++;

      MCOOR[c.distrito_raw] = { unit, user: u.id, regional: c.regional_raw };
    }

    // ASESORÍA
    let asesoresSinCoordinador = 0;
    for (const a of ases) {
      const dcanon = canonizeDistrict(a.distrito_raw);
      const parent = MCOOR[dcanon];

      if (!parent) {
        asesoresSinCoordinador++;
        continue;
      }

      const u = await getOrCreateUser(client, a, parent.unit, parent.regional, parent.user);
      if (u.action === "created") usersCreated++;
      else usersUpdated++;
    }

    await client.query("COMMIT");

    return {
      ok: true,
      message: "Sync Presupuesto Jerarquía -> Users/OrgUnits completado",
      period: per.str,
      period_year: yy,
      period_month: mm,
      input_rows: rows.length,
      users_created: usersCreated,
      users_updated: usersUpdated,
      asesores_sin_coordinador: asesoresSinCoordinador,
      reset_all_users: !!reset_all_users
    };
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("[SYNC PJ -> USERS ERROR]", err);
    throw err;
  } finally {
    client.release();
  }
}

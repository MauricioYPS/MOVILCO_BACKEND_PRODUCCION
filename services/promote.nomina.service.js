// services/promote.nomina.service.js
import pool from "../config/database.js";

function norm(str) {
  return String(str ?? "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

// true/false/null
function parseContratado(v) {
  const s = norm(v);
  if (!s) return null;

  const yes = ["si", "s", "yes", "true", "1", "activo", "vigente", "contratado"];
  const no = ["no", "n", "false", "0", "retirado", "inactivo", "no contratado"];

  if (yes.includes(s)) return true;
  if (no.includes(s)) return false;
  return null;
}

// Superposición del contrato con el mes (YYYY, MM)
function overlapsMonth(contract_start, contract_end, year, month) {
  const periodStart = new Date(Date.UTC(year, month - 1, 1));
  const periodEnd = new Date(Date.UTC(year, month, 0));

  const cs = contract_start ? new Date(contract_start) : new Date("1900-01-01T00:00:00Z");
  const ce = contract_end ? new Date(contract_end) : new Date("2999-12-31T00:00:00Z");

  return cs <= periodEnd && ce >= periodStart;
}

async function findOrgUnitForDistrict(client, district) {
  const q = `%${district}%`;

  // 1) COORDINACION
  {
    const { rows } = await client.query(
      `SELECT id
       FROM core.org_units
       WHERE unit_type = 'COORDINACION' AND name ILIKE $1
       ORDER BY id
       LIMIT 1`,
      [q]
    );
    if (rows[0]) return rows[0].id;
  }

  // 2) DIRECCION
  {
    const { rows } = await client.query(
      `SELECT id
       FROM core.org_units
       WHERE unit_type = 'DIRECCION' AND name ILIKE $1
       ORDER BY id
       LIMIT 1`,
      [q]
    );
    if (rows[0]) return rows[0].id;
  }

  return null;
}

async function getFallbackOrgUnitId(client) {
  {
    const { rows } = await client.query(
      `SELECT id
       FROM core.org_units
       WHERE unit_type = 'DIRECCION'
       ORDER BY id
       LIMIT 1`
    );
    if (rows[0]) return rows[0].id;
  }
  {
    const { rows } = await client.query(
      `SELECT id
       FROM core.org_units
       ORDER BY id
       LIMIT 1`
    );
    if (rows[0]) return rows[0].id;
  }
  throw new Error("No existe ningún registro en core.org_units para usar como fallback.");
}

async function getUserByDocument(client, document_id) {
  const { rows } = await client.query(
    `SELECT id, email, active
     FROM core.users
     WHERE document_id = $1
     LIMIT 1`,
    [document_id]
  );
  return rows[0] || null;
}

async function createUserFromNomina(client, payload) {
  const {
    org_unit_id,
    name,
    email,
    phone,
    role,
    password_hash,
    document_id,
    district,
    district_claro,
    contract_start,
    contract_end,
    contratado_flag
  } = payload;

  const safeEmail = email ? String(email).toLowerCase() : `${document_id || "sinid"}@movilco.local`;

  const { rows } = await client.query(
    `INSERT INTO core.users
      (org_unit_id, name, email, phone, role, password_hash,
       document_id, district, district_claro, contract_start, contract_end,
       active, contratado)
     VALUES ($1,$2,$3,$4,$5,$6,
             $7,$8,$9,$10,$11,
             true, $12)
     RETURNING id`,
    [
      org_unit_id,
      name,
      safeEmail,
      phone ?? null,
      role || "ASESORIA",
      password_hash || "pending-hash",
      document_id || null,
      district || null,
      district_claro || null,
      contract_start || null,
      contract_end || null,
      contratado_flag === true // info, no gobierna active
    ]
  );

  return rows[0].id;
}

async function updateUserFromNomina(client, userId, payload) {
  const {
    org_unit_id,
    name,
    email,
    phone,
    role,
    document_id,
    district,
    district_claro,
    contract_start,
    contract_end,
    contratado_flag
  } = payload;

  const safeEmail = email ? String(email).toLowerCase() : null;

  await client.query(
    `UPDATE core.users
     SET org_unit_id    = COALESCE($1, org_unit_id),
         name           = COALESCE($2, name),
         email          = COALESCE($3, email),
         phone          = COALESCE($4, phone),
         role           = COALESCE($5, role),
         document_id    = COALESCE($6, document_id),
         district       = COALESCE($7, district),
         district_claro = COALESCE($8, district_claro),
         contract_start = COALESCE($9, contract_start),
         contract_end   = COALESCE($10, contract_end),
         contratado     = COALESCE($11, contratado),
         updated_at     = now()
     WHERE id = $12`,
    [
      org_unit_id || null,
      name || null,
      safeEmail,
      phone ?? null,
      role || null,
      document_id || null,
      district || null,
      district_claro || null,
      contract_start || null,
      contract_end || null,
      (contratado_flag === true || contratado_flag === false) ? contratado_flag : null,
      userId
    ]
  );
}

async function upsertUserMonthly(client, userId, year, month, metric) {
  const {
    presupuesto_mes,
    dias_laborados,
    prorrateo,
    estado_envio_presupuesto,
    novedad,
    activo_en_periodo
  } = metric;

  await client.query(
    `INSERT INTO core.user_monthly
      (user_id, period_year, period_month, presupuesto_mes, dias_laborados, prorrateo, estado_envio_presupuesto, novedad, activo_en_periodo)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
     ON CONFLICT (user_id, period_year, period_month)
     DO UPDATE SET
       presupuesto_mes = EXCLUDED.presupuesto_mes,
       dias_laborados  = EXCLUDED.dias_laborados,
       prorrateo       = EXCLUDED.prorrateo,
       estado_envio_presupuesto = EXCLUDED.estado_envio_presupuesto,
       novedad         = EXCLUDED.novedad,
       activo_en_periodo = EXCLUDED.activo_en_periodo,
       updated_at      = now()`,
    [
      userId,
      year,
      month,
      presupuesto_mes ?? null,
      dias_laborados ?? null,
      prorrateo ?? null,
      estado_envio_presupuesto ?? null,
      novedad ?? null,
      (activo_en_periodo === true || activo_en_periodo === false) ? activo_en_periodo : null
    ]
  );
}

export async function promoteNominaFromStaging({ period_year, period_month }) {
  if (!period_year || !period_month) {
    throw new Error("Se requiere period_year y period_month (por ejemplo ?period=2025-10)");
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const fallbackOrgUnitId = await getFallbackOrgUnitId(client);

    const { rows } = await client.query(`
      SELECT
        NULLIF(TRIM(cedula), '')                AS document_id,
        NULLIF(TRIM(nombre_funcionario), '')    AS nombre_funcionario,
        NULLIF(TRIM(contratado), '')            AS contratado,
        NULLIF(TRIM(distrito), '')              AS distrito,
        NULLIF(TRIM(distrito_claro), '')        AS distrito_claro,
        fecha_inicio_contrato,
        fecha_fin_contrato,
        NULLIF(TRIM(novedad), '')               AS novedad,
        presupuesto_mes,
        dias_laborados,
        prorrateo,
        NULLIF(TRIM(estado_envio_presupuesto), '') AS estado_envio_presupuesto
      FROM staging.archivo_nomina
      ORDER BY cedula ASC NULLS LAST
    `);

    let created = 0;
    let updated = 0;
    let noDistrictMatch = 0;
    let skippedNoDocument = 0;

    for (const r of rows) {
      const documentId = r.document_id || null;
      if (!documentId) {
        skippedNoDocument++;
        continue;
      }

      const name = r.nombre_funcionario || "Sin Nombre";
      const contratadoFlag = parseContratado(r.contratado); // true/false/null
      const cStart = r.fecha_inicio_contrato || null;
      const cEnd = r.fecha_fin_contrato || null;
      const district = r.distrito || null;
      const districtClaro = r.distrito_claro || null;

      // Activo mensual (no toca core.users.active)
      // Si contratadoFlag es null, lo tratamos como “sin dato” -> true para no bloquear.
      const contratadoEff = (contratadoFlag === null) ? true : contratadoFlag;
      const activoEnPeriodo = contratadoEff && overlapsMonth(cStart, cEnd, period_year, period_month);

      let org_unit_id = null;
      if (districtClaro) org_unit_id = await findOrgUnitForDistrict(client, districtClaro);
      if (!org_unit_id && district) org_unit_id = await findOrgUnitForDistrict(client, district);

      if (!org_unit_id) {
        noDistrictMatch++;
        org_unit_id = fallbackOrgUnitId;
      }

      const payload = {
        org_unit_id,
        name,
        email: null,
        phone: null,
        role: "ASESORIA",
        password_hash: "pending-hash",
        document_id: documentId,
        district,
        district_claro: districtClaro,
        contract_start: cStart,
        contract_end: cEnd,
        contratado_flag: contratadoFlag
      };

      const user = await getUserByDocument(client, documentId);

      let userId;
      if (!user) {
        userId = await createUserFromNomina(client, payload);
        created++;
      } else {
        await updateUserFromNomina(client, user.id, payload);
        userId = user.id;
        updated++;
      }

      await upsertUserMonthly(client, userId, period_year, period_month, {
        presupuesto_mes: r.presupuesto_mes,
        dias_laborados: r.dias_laborados,
        prorrateo: r.prorrateo,
        estado_envio_presupuesto: r.estado_envio_presupuesto,
        novedad: r.novedad,
        activo_en_periodo: activoEnPeriodo
      });
    }

    await client.query("COMMIT");
    return { created, updated, noDistrictMatch, skippedNoDocument, total: rows.length };
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

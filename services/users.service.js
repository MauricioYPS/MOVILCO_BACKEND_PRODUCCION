import pool from '../config/database.js'

export const VALID_ROLES = ['ADMIN','GERENCIA','DIRECCION','COORDINACION','ASESORIA']
export const isValidRole = (r) => VALID_ROLES.includes(String(r || '').toUpperCase())

/* ============================================================
   BUSCAR USUARIO POR CÉDULA
   ============================================================ */
export async function findUserByDocument(client, document_id) {
  const { rows } = await client.query(
    `SELECT * FROM core.users WHERE document_id = $1 LIMIT 1`,
    [document_id]
  )
  return rows[0] || null
}

/* ============================================================
    UPSERT: CREA O ACTUALIZA USUARIO DESDE PRESUPUESTO/NOMINA
   ============================================================ */
export async function upsertUser(client, data) {
  const {
    id,
    org_unit_id,
    document_id,
    advisor_id = null,
    name,
    email,
    phone = null,
    role,
    active = true,
    district,
    district_claro,
    regional = null,
    contract_start,
    contract_end,
    notes,
    capacity,
    cargo,
    jerarquia,
    presupuesto,
    ejecutado,
    cierre_porcentaje
  } = data

  const password_hash = 'pending-hash'

  if (!id) {
    // CREATE
    const { rows } = await client.query(
      `INSERT INTO core.users (
        org_unit_id,
        document_id,
        advisor_id,
        name,
        email,
        phone,
        role,
        active,
        district,
        district_claro,
        regional,
        contract_start,
        contract_end,
        notes,
        capacity,
        cargo,
        jerarquia,
        presupuesto,
        ejecutado,
        cierre_porcentaje,
        password_hash
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
      RETURNING *`,
      [
        org_unit_id,
        document_id,
        advisor_id,
        name,
        email,
        phone,
        role,
        active,
        district,
        district_claro,
        regional,
        contract_start,
        contract_end,
        notes,
        capacity,
        cargo,
        jerarquia,
        presupuesto,
        ejecutado,
        cierre_porcentaje,
        password_hash
      ]
    )
    return rows[0]
  } else {
    // UPDATE
    const { rows } = await client.query(
      `UPDATE core.users
       SET org_unit_id=$1,
           document_id=$2,
           advisor_id=$3,
           name=$4,
           email=$5,
           phone=$6,
           role=$7,
           active=$8,
           district=$9,
           district_claro=$10,
           regional=$11,
           contract_start=$12,
           contract_end=$13,
           notes=$14,
           capacity=$15,
           cargo=$16,
           jerarquia=$17,
           presupuesto=$18,
           ejecutado=$19,
           cierre_porcentaje=$20,
           updated_at = now()
       WHERE id=$21
       RETURNING *`,
      [
        org_unit_id,
        document_id,
        advisor_id,
        name,
        email,
        phone,
        role,
        active,
        district,
        district_claro,
        regional,
        contract_start,
        contract_end,
        notes,
        capacity,
        cargo,
        jerarquia,
        presupuesto,
        ejecutado,
        cierre_porcentaje,
        id
      ]
    )
    return rows[0]
  }
}

/* ============================================================
   LISTA DE USUARIOS
   ============================================================ */
export async function listUsers({ orgUnitIds = null } = {}) {
  const baseQuery = `
    SELECT
      id,
      org_unit_id,
      document_id,
      advisor_id,
      coordinator_id,
      name,
      email,
      phone,
      role,
      active,
      district,
      district_claro,
      regional,
      cargo,
      capacity,
      jerarquia,
      presupuesto,
      ejecutado,
      cierre_porcentaje,
      contract_start,
      contract_end,
      notes,
      created_at,
      updated_at
    FROM core.users
  `

  if (Array.isArray(orgUnitIds) && orgUnitIds.length > 0) {
    const { rows } = await pool.query(
      `${baseQuery} WHERE org_unit_id = ANY($1::bigint[]) ORDER BY id ASC`,
      [orgUnitIds]
    )
    return rows
  }

  const { rows } = await pool.query(`${baseQuery} ORDER BY id ASC`)
  return rows
}

/* ============================================================
    OBTENER UN USUARIO POR ID
   ============================================================ */
export async function getUserById(id) {
  const { rows } = await pool.query(
    `
    SELECT
      id,
      org_unit_id,
      document_id,
      advisor_id,
      coordinator_id,
      name,
      email,
      phone,
      role,
      active,
      district,
      district_claro,
      regional,
      cargo,
      capacity,
      jerarquia,
      presupuesto,
      ejecutado,
      cierre_porcentaje,
      contract_start,
      contract_end,
      notes,
      created_at,
      updated_at
    FROM core.users
    WHERE id = $1
    `,
    [id]
  )
  return rows[0] || null
}

/* ============================================================
    VALIDACIONES
   ============================================================ */
export async function orgUnitExists(org_unit_id) {
  const { rows } = await pool.query(
    `SELECT 1 FROM core.org_units WHERE id = $1`,
    [org_unit_id]
  )
  return rows.length > 0
}

export async function emailInUse(email, ignoreId = null) {
  if (ignoreId) {
    const { rows } = await pool.query(
      `SELECT 1 FROM core.users WHERE email = $1 AND id <> $2`,
      [email, ignoreId]
    )
    return rows.length > 0
  }

  const { rows } = await pool.query(
    `SELECT 1 FROM core.users WHERE email = $1`,
    [email]
  )
  return rows.length > 0
}
/* ============================================================
    CREATE USER
   ============================================================ */
export async function createUser({
  org_unit_id,
  document_id,
  advisor_id,
  coordinator_id = null,
  name,
  email,
  phone,
  role,
  password_hash = 'pending-hash'
}) {
  const { rows } = await pool.query(
    `
    INSERT INTO core.users (
      org_unit_id,
      document_id,
      advisor_id,
      coordinator_id,
      name,
      email,
      phone,
      role,
      password_hash
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
    RETURNING
      id,
      org_unit_id,
      document_id,
      advisor_id,
      coordinator_id,
      name,
      email,
      phone,
      role,
      active,
      district,
      district_claro,
      regional,
      cargo,
      capacity,
      jerarquia,
      presupuesto,
      ejecutado,
      cierre_porcentaje,
      contract_start,
      contract_end,
      notes,
      created_at,
      updated_at
    `,
    [
      org_unit_id,
      document_id,
      advisor_id,
      coordinator_id,
      name.trim(),
      email.toLowerCase(),
      phone ?? null,
      String(role).toUpperCase(),
      password_hash
    ]
  )
  return rows[0]
}

/* ============================================================
    UPDATE USER
   ============================================================ */
export async function updateUser(
  id,
  {
    org_unit_id,
    document_id,
    advisor_id,
    coordinator_id = null,
    name,
    email,
    phone,
    role,
    active
  }
) {
  const { rows } = await pool.query(
    `
    UPDATE core.users
    SET org_unit_id = $1,
        document_id = $2,
        advisor_id = $3,
        coordinator_id = $4,
        name = $5,
        email = $6,
        phone = $7,
        role = $8,
        active = $9,
        updated_at = now()
    WHERE id = $10
    RETURNING
      id,
      org_unit_id,
      document_id,
      advisor_id,
      coordinator_id,
      name,
      email,
      phone,
      role,
      active,
      district,
      district_claro,
      regional,
      cargo,
      capacity,
      jerarquia,
      presupuesto,
      ejecutado,
      cierre_porcentaje,
      contract_start,
      contract_end,
      notes,
      created_at,
      updated_at
    `,
    [
      org_unit_id,
      document_id,
      advisor_id,
      coordinator_id,
      name.trim(),
      email.toLowerCase(),
      phone ?? null,
      String(role).toUpperCase(),
      !!active,
      id
    ]
  )
  return rows[0]
}


/* ===========================================================
    DELETE USER
   ============================================================ */
export async function deleteUser(id) {
  await pool.query(`DELETE FROM core.users WHERE id = $1`, [id])
  return true
}

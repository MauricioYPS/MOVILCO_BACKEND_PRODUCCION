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

  if (!id) {
    // CREATE sin password_hash (¡NO tocar contraseñas!)
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
        cierre_porcentaje
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
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
        cierre_porcentaje
      ]
    )
    return rows[0]
  } else {
    // UPDATE (NO se toca password_hash)
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
export async function listUsers({ orgUnitIds = null, includeInactive = false } = {}) {
  const baseQuery = `
    WITH org AS (
      SELECT 
        u.id AS user_id,
        u.org_unit_id,
        u.document_id,
        u.advisor_id,
        u.coordinator_id,
        u.name,
        u.email,
        u.phone,
        u.role,
        u.active,
        u.district,
        u.district_claro,
        u.regional,
        u.cargo,
        u.capacity,
        u.jerarquia,
        u.presupuesto,
        u.ejecutado,
        u.cierre_porcentaje,
        u.contract_start,
        u.contract_end,
        u.notes,
        u.created_at,
        u.updated_at,

        ou.id AS unit_id,
        ou.parent_id AS unit_parent_id,
        ou.unit_type AS unit_type,

        d.id AS direccion_unit_id,
        d.parent_id AS direccion_parent_id,
        d.unit_type AS direccion_type,

        g.id AS gerencia_unit_id,
        g.unit_type AS gerencia_type
      FROM core.users u
      LEFT JOIN core.org_units ou ON ou.id = u.org_unit_id
      LEFT JOIN core.org_units d  ON d.id = ou.parent_id
      LEFT JOIN core.org_units g  ON g.id = d.parent_id
      ${includeInactive ? '' : 'WHERE u.active = true'}
    ),

    resolved_parent AS (
      SELECT 
        o.*,
        CASE 
          WHEN o.jerarquia = 'ASESORIA' THEN (
            SELECT id FROM core.users cu 
            WHERE cu.org_unit_id = o.unit_parent_id LIMIT 1
          )
          WHEN o.jerarquia = 'COORDINACION' THEN (
            SELECT id FROM core.users du
            WHERE du.org_unit_id = o.direccion_unit_id LIMIT 1
          )
          WHEN o.jerarquia = 'DIRECCION' THEN (
            SELECT id FROM core.users gu
            WHERE gu.org_unit_id = o.gerencia_unit_id LIMIT 1
          )
          ELSE NULL
        END AS parent_id_normalized
      FROM org o
    )

    SELECT * FROM resolved_parent
  `;

  if (Array.isArray(orgUnitIds) && orgUnitIds.length > 0) {
    const { rows } = await pool.query(
      `${baseQuery} WHERE org_unit_id = ANY($1::bigint[]) ORDER BY user_id ASC`,
      [orgUnitIds]
    );
    return rows;
  }

  const { rows } = await pool.query(`${baseQuery} ORDER BY user_id ASC`);
  return rows;
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
    CREATE USER (para auth, no importadores)
   ============================================================ */
/* ============================================================
    CREATE USER (para auth, no importadores)
    Ahora guarda también district/district_claro y demás campos
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
  active = true,

  district = null,
  district_claro = null,
  regional = null,
  cargo = null,
  capacity = null,
  jerarquia = null,
  contract_start = null,
  contract_end = null,
  notes = null,

  password_hash = null // opcional
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
      active,
      district,
      district_claro,
      regional,
      cargo,
      capacity,
      jerarquia,
      contract_start,
      contract_end,
      notes,
      password_hash
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
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
      name?.trim(),
      String(email || "").toLowerCase(),
      phone ?? null,
      String(role || "").toUpperCase(),
      !!active,
      district,
      district_claro,
      regional,
      cargo,
      capacity,
      jerarquia,
      contract_start,
      contract_end,
      notes,
      password_hash
    ]
  );

  return rows[0];
}

/* ============================================================
    UPDATE USER (no modifica contraseñas)
    Ahora también permite actualizar district/district_claro, etc.
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
    active,

    district = null,
    district_claro = null,
    regional = null,
    cargo = null,
    capacity = null,
    jerarquia = null,
    contract_start = null,
    contract_end = null,
    notes = null
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

        district = $10,
        district_claro = $11,
        regional = $12,
        cargo = $13,
        capacity = $14,
        jerarquia = $15,
        contract_start = $16,
        contract_end = $17,
        notes = $18,

        updated_at = now()
    WHERE id = $19
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
      name?.trim(),
      String(email || "").toLowerCase(),
      phone ?? null,
      String(role || "").toUpperCase(),
      !!active,

      district,
      district_claro,
      regional,
      cargo,
      capacity,
      jerarquia,
      contract_start,
      contract_end,
      notes,

      id
    ]
  );

  return rows[0];
}


/* ============================================================
    DELETE USER (solo para admin, borra físicamente)
   ============================================================ */
export async function deleteUser(id) {
  await pool.query(`DELETE FROM core.users WHERE id = $1`, [id])
  return true
}
/* ============================================================
    SOFT DELETE USER (desactiva sin borrar)
   ============================================================ */
export async function deactivateUser(id) {
  await pool.query(
    `
    UPDATE core.users
    SET active = false,
        coordinator_id = NULL,
        updated_at = now()
    WHERE id = $1
    `,
    [id]
  )
  return true
}

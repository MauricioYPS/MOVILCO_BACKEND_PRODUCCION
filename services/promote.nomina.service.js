import pool from '../config/database.js'

function norm(str) {
  return String(str ?? '')
    .trim()
    .toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
}

function toBool(v) {
  const s = norm(v)
  return ['si','s','yes','true','1'].includes(s)
}

// Dadas fechas y un periodo (YYYY, MM), revisa si hay superposición
// entre [contract_start, contract_end] y el mes [periodStart, periodEnd].
function overlapsMonth(contract_start, contract_end, year, month) {
  if (!year || !month) return true // si no hay periodo, asumimos "activo" (fallback)
  // ventana del mes (primer y último día)
  const periodStart = new Date(Date.UTC(year, month - 1, 1))
  const periodEnd   = new Date(Date.UTC(year, month, 0)) // día 0 del siguiente mes = último día del mes

  // si no viene start -> asumimos -infinito; si no viene end -> +infinito
  const cs = contract_start ? new Date(contract_start) : new Date('1900-01-01T00:00:00Z')
  const ce = contract_end   ? new Date(contract_end)   : new Date('2999-12-31T00:00:00Z')

  // hay superposición si el inicio del contrato es <= fin del mes
  // y el fin del contrato es >= inicio del mes
  return (cs <= periodEnd) && (ce >= periodStart)
}

// Busca el org_unit_id de la COORDINACION por el nombre de distrito (substring)
// Si no encuentra coordinación, intentamos DIRECCION; si no, null.
async function findOrgUnitForDistrict(client, district) {
  const q = `%${district}%`
  // 1) Coincidencia con COORDINACION
  {
    const { rows } = await client.query(
      `SELECT id FROM core.org_units
       WHERE unit_type = 'COORDINACION' AND name ILIKE $1
       ORDER BY id LIMIT 1`,
       [q]
    )
    if (rows[0]) return rows[0].id
  }
  // 2) Coincidencia con DIRECCION (fallback)
  {
    const { rows } = await client.query(
      `SELECT id FROM core.org_units
       WHERE unit_type = 'DIRECCION' AND name ILIKE $1
       ORDER BY id LIMIT 1`,
       [q]
    )
    if (rows[0]) return rows[0].id
  }
  return null
}

// Obtiene usuario por document_id (cédula)
async function getUserByDocument(client, document_id) {
  const { rows } = await client.query(
    `SELECT id, email FROM core.users WHERE document_id = $1 LIMIT 1`,
    [document_id]
  )
  return rows[0] || null
}

// Crea un usuario "asesor" base (email sintético si no viene)
async function createUserFromNomina(client, payload) {
  const {
    org_unit_id, name, email, phone, role,
    password_hash, document_id, district, district_claro,
    contract_start, contract_end, active, notes
  } = payload

  // Si no viene email, generamos uno sintético para cumplir UNIQUE NOT NULL
  const safeEmail = email
    ? String(email).toLowerCase()
    : `${document_id || 'sinid'}@movilco.local`

  const { rows } = await client.query(
    `INSERT INTO core.users
      (org_unit_id, name, email, phone, role, password_hash,
       document_id, district, district_claro, contract_start, contract_end, active, notes)
     VALUES ($1,$2,$3,$4,$5,$6,
             $7,$8,$9,$10,$11,$12,$13)
     RETURNING id`,
    [
      org_unit_id, name, safeEmail, phone ?? null, role || 'ASESORIA', password_hash || 'pending-hash',
      document_id || null, district || null, district_claro || null, contract_start || null, contract_end || null,
      active === true, notes || null
    ]
  )
  return rows[0].id
}

// Actualiza usuario existente
async function updateUserFromNomina(client, userId, payload) {
  const {
    org_unit_id, name, email, phone, role,
    document_id, district, district_claro,
    contract_start, contract_end, active, notes
  } = payload

  const safeEmail = email ? String(email).toLowerCase() : null

  await client.query(
    `UPDATE core.users
     SET org_unit_id   = COALESCE($1, org_unit_id),
         name          = COALESCE($2, name),
         email         = COALESCE($3, email),
         phone         = COALESCE($4, phone),
         role          = COALESCE($5, role),
         document_id   = COALESCE($6, document_id),
         district      = COALESCE($7, district),
         district_claro= COALESCE($8, district_claro),
         contract_start= COALESCE($9, contract_start),
         contract_end  = COALESCE($10, contract_end),
         active        = COALESCE($11, active),
         notes         = COALESCE($12, notes),
         updated_at    = now()
     WHERE id = $13`,
     [
       org_unit_id || null, name || null, safeEmail, phone ?? null, role || null,
       document_id || null, district || null, district_claro || null,
       contract_start || null, contract_end || null, active === true || active === false ? active : null,
       notes || null, userId
     ]
  )
}

// Inserta/actualiza métrica mensual (upsert por UNIQUE user_id,period)
async function upsertUserMonthly(client, userId, year, month, metric) {
  const {
    presupuesto_mes, dias_laborados, prorrateo, estado_envio_presupuesto, novedad
  } = metric

  await client.query(
    `INSERT INTO core.user_monthly
      (user_id, period_year, period_month, presupuesto_mes, dias_laborados, prorrateo, estado_envio_presupuesto, novedad)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
     ON CONFLICT (user_id, period_year, period_month)
     DO UPDATE SET
       presupuesto_mes = EXCLUDED.presupuesto_mes,
       dias_laborados  = EXCLUDED.dias_laborados,
       prorrateo       = EXCLUDED.prorrateo,
       estado_envio_presupuesto = EXCLUDED.estado_envio_presupuesto,
       novedad         = EXCLUDED.novedad,
       updated_at      = now()`,
    [
      userId, year, month,
      presupuesto_mes ?? null,
      dias_laborados ?? null,
      prorrateo ?? null,
      estado_envio_presupuesto ?? null,
      novedad ?? null
    ]
  )
}

// Lógica principal de promoción
export async function promoteNominaFromStaging({ period_year, period_month }) {
  if (!period_year || !period_month) {
    throw new Error('Se requiere period_year y period_month (por ejemplo ?period=2025-11)')
  }

  const client = await pool.connect()
  try {
    await client.query('BEGIN')

    // 1) Tomamos todo lo que haya en staging.archivo_nomina
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
    `)

    let created = 0
    let updated = 0
    let noDistrictMatch = 0

    for (const r of rows) {
      // Normalizamos lo esencial
      const documentId = r.document_id || null
      const name       = r.nombre_funcionario || 'Sin Nombre'
      const contratado = toBool(r.contratado) // SI/NO
      const cStart     = r.fecha_inicio_contrato || null
      const cEnd       = r.fecha_fin_contrato   || null
      const district   = r.distrito || null
      const districtClaro = r.distrito_claro || null
      const novedad    = r.novedad || null

      // Activo si: contratado=SI y el contrato se solapa con el mes dado
      const active = contratado && overlapsMonth(cStart, cEnd, period_year, period_month)

      // Ubicar org_unit_id (coordinación preferentemente)
      let org_unit_id = null
      if (districtClaro) {
        org_unit_id = await findOrgUnitForDistrict(client, districtClaro)
      }
      if (!org_unit_id && district) {
        org_unit_id = await findOrgUnitForDistrict(client, district)
      }
      if (!org_unit_id) {
        noDistrictMatch++
      }

      // Email/phone no vienen en nómina; dejamos null/sintético.
      const payload = {
        org_unit_id,
        name,
        email: null,
        phone: null,
        role: 'ASESORIA',
        password_hash: 'pending-hash',
        document_id: documentId,
        district: district,
        district_claro: districtClaro,
        contract_start: cStart,
        contract_end: cEnd,
        active,
        notes: novedad
      }

      // Upsert por document_id
      let user = null
      if (documentId) {
        user = await getUserByDocument(client, documentId)
      }

      let userId
      if (!user) {
        if (!org_unit_id) {
          // No district match means we cannot satisfy the NOT NULL org_unit constraint.
          continue
        }
        userId = await createUserFromNomina(client, payload)
        created++
      } else {
        await updateUserFromNomina(client, user.id, payload)
        userId = user.id
        updated++
      }

      // Métrica mensual (si viene info relevante)
      await upsertUserMonthly(client, userId, period_year, period_month, {
        presupuesto_mes: r.presupuesto_mes,
        dias_laborados : r.dias_laborados,
        prorrateo      : r.prorrateo,
        estado_envio_presupuesto: r.estado_envio_presupuesto,
        novedad
      })
    }

    await client.query('COMMIT')
    return { created, updated, noDistrictMatch, total: rows.length }
  } catch (e) {
    await client.query('ROLLBACK')
    throw e
  } finally {
    client.release()
  }
}

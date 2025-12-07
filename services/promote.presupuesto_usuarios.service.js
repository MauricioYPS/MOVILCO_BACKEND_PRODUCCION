// services/promote.presupuesto_usuarios.service.js
import pool from '../config/database.js'
import { upsertUser, findUserByDocument } from './users.service.js'
import { emailInUse } from './users.service.js'

// Normalizar texto
function normalize(v) {
  return String(v ?? '')
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
}

// Generar email único garantizado
async function generateUniqueEmail(client, cedula, correoRaw) {
  // Si viene correo real y válido → usarlo si no está repetido
  if (correoRaw && correoRaw.includes('@')) {
    const exists = await emailInUse(correoRaw)
    if (!exists) return correoRaw.toLowerCase()
  }

  // Si no vino correo → lo generamos
  let base = `${cedula}@auto.movilco`
  let email = base
  let counter = 1

  while (await emailInUse(email)) {
    email = `${cedula}+${counter}@auto.movilco`
    counter++
  }

  return email
}

// Copiada de promote.presupuesto.service.js
async function findOrgUnitByName(client, name) {
  const q = `%${name}%`
  const { rows } = await client.query(
    `SELECT id, unit_type FROM core.org_units
     WHERE name ILIKE $1
     ORDER BY CASE
       WHEN unit_type='GERENCIA' THEN 1
       WHEN unit_type='DIRECCION' THEN 2
       WHEN unit_type='COORDINACION' THEN 3
       ELSE 4 END
     LIMIT 1`,
    [q]
  )
  return rows[0] || null
}

export async function promotePresupuestoUsuarios() {
  const client = await pool.connect()
  try {
    await client.query('BEGIN')

    const { rows } = await client.query(`
      SELECT
        NULLIF(TRIM(jerarquia),'') AS jerarquia,
        NULLIF(TRIM(cargo),'') AS cargo,
        NULLIF(TRIM(cedula),'') AS cedula,
        NULLIF(TRIM(nombre),'') AS nombre,
        NULLIF(TRIM(distrito),'') AS distrito,
        NULLIF(TRIM(regional),'') AS regional,
        fecha_inicio,
        fecha_fin,
        NULLIF(TRIM(novedades),'') AS novedades,
        NULLIF(TRIM(presupuesto),'') AS presupuesto,
        NULLIF(TRIM(capacidad),'') AS capacidad,
        NULLIF(TRIM(telefono),'') AS telefono,
        NULLIF(TRIM(correo),'') AS correo
      FROM staging.presupuesto_usuarios
    `)

    let created = 0
    let updated = 0
    let noDistrict = 0

    for (const r of rows) {
      if (!r.cedula) continue

      // 1. Resolver unidad organizacional
      let unit = null
      if (r.distrito) unit = await findOrgUnitByName(client, r.distrito)
      if (!unit) {
        noDistrict++
        continue
      }

      // 2. Buscar usuario existente por cédula
      const existing = await findUserByDocument(client, r.cedula)

      // 3. Email único
      const safeEmail = await generateUniqueEmail(client, r.cedula, r.correo)

      // 4. Payload consistente
      const payload = {
        org_unit_id: unit.id,
        role: r.jerarquia?.toUpperCase().includes('COORD') ? 'COORDINACION' : 'ASESORIA',
        name: r.nombre,
        document_id: r.cedula,
        district: r.distrito,
        regional: r.regional,
        email: safeEmail,
        phone: r.telefono || null,
        capacity: r.capacidad || null,
        contract_start: r.fecha_inicio,
        contract_end: r.fecha_fin,
        active: true,
        notes: r.novedades
      }

      // 5. Crear o actualizar
      if (!existing) {
        await upsertUser(client, payload)
        created++
      } else {
        await upsertUser(client, { ...payload, id: existing.id })
        updated++
      }
    }

    await client.query('COMMIT')
    return { created, updated, noDistrict, total: rows.length }

  } catch (e) {
    await client.query('ROLLBACK')
    throw e
  } finally {
    client.release()
  }
}

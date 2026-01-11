// services/promote.nomina.service.js
import pool from "../config/database.js";

/** ---------- Helpers base ---------- */
function onlyDigits(v) {
  const s = String(v ?? "").replace(/\D/g, "").trim();
  return s === "" ? null : s;
}

function normSpaces(v) {
  const s = String(v ?? "").replace(/\s+/g, " ").trim();
  return s === "" ? null : s;
}

function normalizeForMatch(v) {
  // Para comparar nombres de org_units sin depender de unaccent del DB
  const s = String(v ?? "")
    .trim()
    .toUpperCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "") // sin acentos
    .replace(/\s+/g, " ");
  return s === "" ? null : s;
}

/**
 * Quita prefijos típicos que vienen en nómina:
 * - "ZONA "
 * - "DISTRITO "
 * - "ZONA DISTRITO "
 * - "ZONAS "
 * - "ZONA: "
 * - "DISTRITO: "
 */
function stripCommonPrefixes(v) {
  const s = normalizeForMatch(v);
  if (!s) return null;

  // conservador: no borrar de más
  const patterns = [
    /^ZONA\s+DISTRITO\s+/,
    /^ZONAS\s+/,
    /^ZONA:\s+/,
    /^DISTRITO:\s+/,
    /^ZONA\s+/,
    /^DISTRITO\s+/,
  ];

  let out = s;
  for (const re of patterns) out = out.replace(re, "");
  out = out.replace(/\s+/g, " ").trim();
  return out === "" ? null : out;
}

/**
 * - Si vienen ambos (district y district_claro), los respeta aunque sean distintos.
 * - Si solo viene uno, lo replica en ambos.
 */
function mergeDistricts(district, districtClaro) {
  const d = normSpaces(district);
  const dc = normSpaces(districtClaro);

  if (d && dc) return { district: d, district_claro: dc };

  const merged = d || dc || null;
  return { district: merged, district_claro: merged };
}

/** ---------- Validaciones email/user ---------- */
async function emailIsFreeForDocument(client, email, documentId) {
  if (!email) return true;
  const { rows } = await client.query(
    `
    SELECT 1
    FROM core.users
    WHERE email = $1
      AND document_id IS DISTINCT FROM $2
    LIMIT 1
    `,
    [email, documentId]
  );
  return rows.length === 0;
}

async function getUserByDocument(client, documentId) {
  const { rows } = await client.query(
    `
    SELECT id, document_id, email, org_unit_id, coordinator_id, regional
    FROM core.users
    WHERE document_id = $1
    LIMIT 1
    `,
    [documentId]
  );
  return rows[0] || null;
}

/** ---------- Resolver org_unit_id robusto ---------- */
/**
 * Estrategia:
 * - toma candidates: district_claro y district
 * - genera variantes:
 *    A) normalizado completo
 *    B) normalizado sin prefijos (ZONA/DISTRITO)
 * - intenta:
 *    1) match exacto normalizado contra org_units
 *    2) match "contiene" (candidate contiene name o name contiene candidate)
 */
async function resolveOrgUnitIdByDistrict(client, district, districtClaro) {
  const rawCandidates = [districtClaro, district].map(normSpaces).filter(Boolean);
  if (!rawCandidates.length) return null;

  const variants = [];
  for (const c of rawCandidates) {
    const a = normalizeForMatch(c);
    const b = stripCommonPrefixes(c);
    if (a) variants.push(a);
    if (b && b !== a) variants.push(b);
  }

  // 1) EXACTO NORMALIZADO
  for (const v of variants) {
    const { rows } = await client.query(
      `
      SELECT id
      FROM core.org_units
      WHERE unit_type = 'COORDINACION'
        AND UPPER(translate(name,
          'ÁÉÍÓÚÜÑáéíóúüñ',
          'AEIOUUNAEIOUUN'
        )) = $1
      ORDER BY id
      LIMIT 1
      `,
      [v]
    );
    if (rows[0]?.id) return rows[0].id;
  }

  // 2) CONTIENE (ej: "ZONA ALFONSO LOPEZ" vs "ALFONSO LOPEZ")
  for (const v of variants.sort((a, b) => b.length - a.length)) {
    const like = `%${v}%`;
    const { rows } = await client.query(
      `
      SELECT id, name
      FROM core.org_units
      WHERE unit_type = 'COORDINACION'
        AND (
          UPPER(translate(name,
            'ÁÉÍÓÚÜÑáéíóúüñ',
            'AEIOUUNAEIOUUN'
          )) ILIKE $1
          OR $2 ILIKE '%' || UPPER(translate(name,
            'ÁÉÍÓÚÜÑáéíóúüñ',
            'AEIOUUNAEIOUUN'
          )) || '%'
        )
      ORDER BY length(name) DESC, id ASC
      LIMIT 1
      `,
      [like, v]
    );
    if (rows[0]?.id) return rows[0].id;
  }

  return null;
}

/** ---------- Resolver coordinator_id opcional ---------- */
async function resolveCoordinatorIdByOrgUnit(client, orgUnitId) {
  if (!orgUnitId) return null;
  const { rows } = await client.query(
    `
    SELECT id
    FROM core.users
    WHERE org_unit_id = $1
      AND (
        role = 'COORDINACION'
        OR jerarquia = 'COORDINACION'
        OR cargo ILIKE '%COORDINADOR%'
      )
    ORDER BY id ASC
    LIMIT 1
    `,
    [orgUnitId]
  );
  return rows[0]?.id ?? null;
}

/** ---------- Resolver regional (lo que faltaba) ---------- */
/**
 * Orden de preferencia:
 *  A) regional del coordinador (si existe)
 *  B) regional de cualquier usuario previo en esa misma COORDINACION (org_unit_id)
 *  C) null (si no hay cómo inferirlo)
 */
async function resolveRegional(client, orgUnitId, coordinatorId) {
  // A) Preferir regional del coordinador
  if (coordinatorId) {
    const { rows } = await client.query(
      `
      SELECT regional
      FROM core.users
      WHERE id = $1
      LIMIT 1
      `,
      [coordinatorId]
    );
    const r = rows[0]?.regional;
    if (r) return r;
  }

  // B) Fallback: buscar cualquier usuario en la misma coordinación con regional definido
  if (orgUnitId) {
    const { rows } = await client.query(
      `
      SELECT regional
      FROM core.users
      WHERE org_unit_id = $1
        AND regional IS NOT NULL
      ORDER BY updated_at DESC NULLS LAST, created_at DESC
      LIMIT 1
      `,
      [orgUnitId]
    );
    const r = rows[0]?.regional;
    if (r) return r;
  }

  return null;
}

/** ---------- Promote principal ---------- */
export async function promoteNominaFromStaging() {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const { rows } = await client.query(`
      SELECT
        NULLIF(TRIM(cedula), '')              AS document_id,
        NULLIF(TRIM(nombre_funcionario), '')  AS nombre_funcionario,
        NULLIF(TRIM(distrito), '')            AS distrito,
        NULLIF(TRIM(distrito_claro), '')      AS distrito_claro,
        fecha_inicio_contrato                 AS fecha_inicio_contrato,
        NULLIF(TRIM(telefono), '')            AS telefono,
        NULLIF(TRIM(correo), '')              AS correo
      FROM staging.archivo_nomina
      ORDER BY cedula ASC NULLS LAST
    `);

    let total = rows.length;
    let inserted = 0;
    let updated = 0;

    let skippedNoDocument = 0;
    let skippedMissingEmail = 0;
    let emailConflicts = 0;
    let skippedNoOrgUnit = 0;
    let skippedNoRegional = 0;

    const email_conflicts_sample = [];
    const no_orgunit_sample = [];
    const no_regional_sample = [];

    for (const r of rows) {
      const documentId = onlyDigits(r.document_id);
      if (!documentId) {
        skippedNoDocument++;
        continue;
      }

      const name = normSpaces(r.nombre_funcionario) || "SIN NOMBRE";
      const phone = normSpaces(r.telefono);
      const email = normSpaces(r.correo);
      const contractStart = r.fecha_inicio_contrato || null;

      // IMPORTANTE: respeta ambos campos si vienen distintos
      const { district, district_claro } = mergeDistricts(r.distrito, r.distrito_claro);

      // 1) Resolver org_unit_id robusto (COORDINACION)
      const orgUnitId = await resolveOrgUnitIdByDistrict(client, district, district_claro);

      if (!orgUnitId) {
        skippedNoOrgUnit++;
        if (no_orgunit_sample.length < 100) {
          no_orgunit_sample.push({
            document_id: documentId,
            nombre: name,
            district,
            district_claro,
            motivo:
              "No se pudo resolver org_unit_id por distrito/distrito_claro (match robusto falló).",
          });
        }
        continue;
      }

      if (!email) skippedMissingEmail++;

      const emailOk = email ? await emailIsFreeForDocument(client, email, documentId) : true;

      // 2) Buscar usuario existente
      const existing = await getUserByDocument(client, documentId);

      // 3) coordinator_id (opcional). Si ya existe usuario, preferimos su coordinator_id.
      //    Si no existe, intentamos inferirlo por org_unit_id.
      const inferredCoordinatorId = await resolveCoordinatorIdByOrgUnit(client, orgUnitId);
      const coordinatorId =
        existing?.coordinator_id ?? inferredCoordinatorId ?? null;

      // 4) Resolver regional (lo que faltaba)
      const inferredRegional = await resolveRegional(client, orgUnitId, coordinatorId);

      // Auditoría (no bloquea)
      if (!inferredRegional) {
        skippedNoRegional++;
        if (no_regional_sample.length < 100) {
          no_regional_sample.push({
            document_id: documentId,
            nombre: name,
            org_unit_id: orgUnitId,
            coordinator_id: coordinatorId,
            district,
            district_claro,
            motivo:
              "No se pudo inferir regional (coordinador sin regional y no hay usuarios con regional en esa coordinación).",
          });
        }
      }

      if (!existing) {
        // email NOT NULL + UNIQUE: si no hay email o hay conflicto, usamos sintético estable
        const emailToUse = emailOk && email ? email : `${documentId}@no-email.local`;

        if (email && !emailOk) {
          emailConflicts++;
          if (email_conflicts_sample.length < 100) {
            email_conflicts_sample.push({
              document_id: documentId,
              correo: email,
              accion: "INSERT usa email sintético (conflicto UNIQUE)",
            });
          }
        }

        await client.query(
          `
          INSERT INTO core.users (
            org_unit_id,
            name,
            email,
            phone,
            role,
            password_hash,
            active,
            document_id,
            district,
            district_claro,
            contract_start,
            contract_end,
            notes,
            advisor_id,
            regional,
            coordinator_id,
            cargo,
            capacity,
            jerarquia,
            presupuesto,
            ejecutado,
            cierre_porcentaje,
            contract_status,
            contratado,
            created_source,
            en_presupuesto
          ) VALUES (
            $1,$2,$3,$4,
            'ASESORIA',
            NULL,
            true,
            $5,$6,$7,$8,
            NULL,
            NULL,
            NULL,
            $9,
            $10,
            'ASESOR COMERCIAL',
            NULL,
            'ASESORIA',
            0,0,0,
            NULL,
            true,
            'IMPORT_NOMINA',
            false
          )
          `,
          [
            orgUnitId,
            name,
            emailToUse,
            phone,
            documentId,
            district,
            district_claro,
            contractStart,
            inferredRegional, // puede ser null
            coordinatorId, // puede ser null
          ]
        );

        inserted++;
      } else {
        const setEmail = email && emailOk;

        if (email && !emailOk) {
          emailConflicts++;
          if (email_conflicts_sample.length < 100) {
            email_conflicts_sample.push({
              document_id: documentId,
              correo: email,
              accion: "UPDATE omite email (conflicto UNIQUE)",
            });
          }
        }

        // Reglas Update:
        // - org_unit_id se actualiza (nómina manda)
        // - coordinator_id SOLO si estaba null (COALESCE)
        // - regional SOLO si estaba null (COALESCE)  <-- objetivo
        await client.query(
          `
          UPDATE core.users
          SET
            org_unit_id = $2,
            name = $3,
            phone = COALESCE($4, phone),
            district = COALESCE($5, district),
            district_claro = COALESCE($6, district_claro),
            contract_start = COALESCE($7, contract_start),
            active = true,
            contratado = true,
            role = 'ASESORIA',
            jerarquia = 'ASESORIA',
            cargo = COALESCE(cargo, 'ASESOR COMERCIAL'),
            coordinator_id = COALESCE(coordinator_id, $8),
            regional = COALESCE(regional, $9),
            updated_at = now(),
            email = CASE
              WHEN $10::boolean = true THEN $11
              ELSE email
            END
          WHERE document_id = $1
          `,
          [
            documentId,
            orgUnitId,
            name,
            phone,
            district,
            district_claro,
            contractStart,
            coordinatorId, // asigna solo si estaba null
            inferredRegional, // asigna solo si estaba null
            setEmail,
            email,
          ]
        );

        updated++;
      }
    }

    await client.query("COMMIT");

    return {
      ok: true,
      total,
      inserted,
      updated,
      skippedNoDocument,
      skippedMissingEmail,
      skippedNoOrgUnit,
      skippedNoRegional,
      emailConflicts,
      email_conflicts_sample,
      no_orgunit_sample,
      no_regional_sample,
    };
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

// services/catalog.service.js
import pool from "../config/database.js";

function normalizeSql(col) {
  // upper(trim(col)) pero cuidando null
  return `upper(trim(coalesce(${col}, '')))`;
}

export async function listRegions({ source = "coordinators" } = {}) {
  // Fuente más estable: core.users (coordinadores)
  if (source === "coordinators" || source === "users") {
    const { rows } = await pool.query(`
      SELECT ${normalizeSql("u.regional")} AS value
      FROM core.users u
      WHERE u.regional IS NOT NULL AND trim(u.regional) <> ''
        AND (u.role = 'COORDINACION' OR u.jerarquia = 'COORDINACION')
        AND u.active = true
      GROUP BY 1
      ORDER BY 1
    `);
    return rows.map((r) => r.value).filter(Boolean);
  }

  // Si sí quieres "nomina", entonces aquí DEBE ser la columna regional (no distrito)
  const { rows } = await pool.query(`
    SELECT ${normalizeSql("n.regional")} AS value
    FROM staging.archivo_nomina n
    WHERE n.regional IS NOT NULL AND trim(n.regional) <> ''
      AND upper(coalesce(n.contratado,'')) = 'SI'
    GROUP BY 1
    ORDER BY 1
  `);
  return rows.map((r) => r.value).filter(Boolean);
}


export async function listDistricts({ source = "nomina" } = {}) {
  if (source === "users") {
    const { rows } = await pool.query(`
      SELECT ${normalizeSql("u.district")} AS value
      FROM core.users u
      WHERE u.district IS NOT NULL AND trim(u.district) <> ''
      GROUP BY 1
      ORDER BY 1
    `);
    return rows.map(r => r.value).filter(Boolean);
  }

  // nomina usa "distrito" (tu tabla tiene distrito y distrito_claro)
  const { rows } = await pool.query(`
    SELECT ${normalizeSql("n.distrito")} AS value
    FROM staging.archivo_nomina n
    WHERE n.distrito IS NOT NULL AND trim(n.distrito) <> ''
      AND upper(coalesce(n.contratado,'')) = 'SI'
    GROUP BY 1
    ORDER BY 1
  `);
  return rows.map(r => r.value).filter(Boolean);
}

export async function listDistrictsClaro({ source = "nomina" } = {}) {
  if (source === "users") {
    const { rows } = await pool.query(`
      SELECT ${normalizeSql("u.district_claro")} AS value
      FROM core.users u
      WHERE u.district_claro IS NOT NULL AND trim(u.district_claro) <> ''
      GROUP BY 1
      ORDER BY 1
    `);
    return rows.map(r => r.value).filter(Boolean);
  }

  const { rows } = await pool.query(`
    SELECT ${normalizeSql("n.distrito_claro")} AS value
    FROM staging.archivo_nomina n
    WHERE n.distrito_claro IS NOT NULL AND trim(n.distrito_claro) <> ''
      AND upper(coalesce(n.contratado,'')) = 'SI'
    GROUP BY 1
    ORDER BY 1
  `);
  return rows.map(r => r.value).filter(Boolean);
}

export async function listCoordinators({ activeOnly = true } = {}) {
  const { rows } = await pool.query(
    `
    SELECT
      u.id,
      u.org_unit_id,
      u.document_id,
      u.name,
      u.email,
      u.regional,
      u.district,
      u.district_claro,
      u.active
    FROM core.users u
    WHERE (u.role = 'COORDINACION' OR u.jerarquia = 'COORDINACION')
      ${activeOnly ? "AND u.active = true" : ""}
    ORDER BY u.name ASC
    `
  );
  return rows;
}


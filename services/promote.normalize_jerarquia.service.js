// services/promote.normalize_jerarquia.service.js
import pool from "../config/database.js";

// -------------------------------------------------------------
// HELPER norm()
// -------------------------------------------------------------
function norm(s) {
  if (!s) return "";
  return String(s)
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
}

// -------------------------------------------------------------
// ROOTS — PATRONES JERÁRQUICOS (unificados y corregidos)
// -------------------------------------------------------------
const ROOT_PATTERNS = {
  META: ["META", "LLANOS", "VILLAVICENCIO"],
  MEDELLIN: ["MEDELLIN", "BELLO", "OCCIDENTAL", "NOROCCIDENTE"],
  TOLIMA: ["TOLIMA", "IBAGUE"],
  SANTANDER: ["SANTANDER", "BUCARAMANGA", "FLORIDABLANCA", "CUCUTA"],
  CAUCA: ["CAUCA", "CHIPICHAPE", "YUMBO", "ALFONSO LOPEZ"],
  HUILA: ["HUILA", "NEIVA", "PITALITO"],
  VALLE: ["VALLE", "CALI"],
  CUNDINAMARCA: ["CUN", "CUNDINAMARCA", "CUMMENAL"],
  ORIENTE: ["ORIENTE"],
};

// -------------------------------------------------------------
// Detectar root
// -------------------------------------------------------------
function matchRoot(district) {
  const txt = norm(district);

  for (const root of Object.keys(ROOT_PATTERNS)) {
    const patterns = ROOT_PATTERNS[root];
    for (const p of patterns) {
      if (txt.includes(p)) {
        return root;
      }
    }
  }

  return null;
}

// -------------------------------------------------------------
// NORMALIZACIÓN COMPLETA
// -------------------------------------------------------------
export async function normalizeJerarquia() {

  // -----------------------------------------------------------------
  // 1. LEER TODOS LOS USUARIOS (con su org_unit_id y role)
  // -----------------------------------------------------------------
  const { rows: users } = await pool.query(`
    SELECT id, name, role, district, org_unit_id, coordinator_id
    FROM core.users
    ORDER BY id ASC
  `);

  const gerencias = users.filter(u => u.role === "GERENCIA");
  const direcciones = users.filter(u => u.role === "DIRECCION");
  const coordinaciones = users.filter(u => u.role === "COORDINACION");
  const asesorias = users.filter(u => u.role === "ASESORIA");

  // -----------------------------------------------------------------
  // 2. Construir mapa ROOT → Director que atiende ese root
  // -----------------------------------------------------------------
  const mapDirectorForRoot = {};

  for (const d of direcciones) {
    const root = matchRoot(d.district);

    if (!root) {
      console.warn(`⚠ Director sin root claro: ${d.name} → ${d.district}`);
      continue;
    }

    if (!mapDirectorForRoot[root]) mapDirectorForRoot[root] = [];

    mapDirectorForRoot[root].push(d);
  }

  // -----------------------------------------------------------------
  // 3. Enlazar COORDINADORES → DIRECTORES
  // -----------------------------------------------------------------
  for (const c of coordinaciones) {
    const root = matchRoot(c.district);

    if (!root || !mapDirectorForRoot[root]) {
      console.warn(`⚠ Coordinador sin director asignable: ${c.name} → ${c.district}`);
      continue;
    }

    const targetDirector = mapDirectorForRoot[root][0]; // se toma el primero

    // Actualizar parent_id en su org_unit
    await pool.query(
      `UPDATE core.org_units SET parent_id = $1 WHERE id = $2`,
      [targetDirector.org_unit_id, c.org_unit_id]
    );

    // El coordinador depende del director
    await pool.query(
      `UPDATE core.users SET coordinator_id = $1 WHERE id = $2`,
      [targetDirector.id, c.id]
    );
  }

  // -----------------------------------------------------------------
  // 4. Construir mapa ROOT → Coordinadores
  // -----------------------------------------------------------------
  const mapCoordForRoot = {};

  for (const c of coordinaciones) {
    const root = matchRoot(c.district);

    if (!root) {
      console.warn(`⚠ Coordinador sin root válido: ${c.name} → ${c.district}`);
      continue;
    }

    if (!mapCoordForRoot[root]) mapCoordForRoot[root] = [];
    mapCoordForRoot[root].push(c);
  }

  // -----------------------------------------------------------------
  // 5. Enlazar ASESORES → COORDINADORES
  // -----------------------------------------------------------------
  for (const a of asesorias) {
    const root = matchRoot(a.district);

    if (!root || !mapCoordForRoot[root]) {
      console.warn(`⚠ Asesor sin coordinador posible: ${a.name} → ${a.district}`);
      continue;
    }

    const coord = mapCoordForRoot[root][0]; // coordinador principal del root

    // Asesor cuelga en el org unit del coordinador
    await pool.query(
      `UPDATE core.users SET org_unit_id=$1, coordinator_id=$2 WHERE id=$3`,
      [coord.org_unit_id, coord.id, a.id]
    );
  }

  // -----------------------------------------------------------------
  // 6. Normalizar direcciones colgando de gerencia por si acaso
  // -----------------------------------------------------------------
  const gerenciaPrincipal = gerencias[0] || null;
  if (gerenciaPrincipal) {
    for (const d of direcciones) {
      const root = matchRoot(d.district);

      // si la dirección quedó sin parent, se cuelga a gerencia principal
      await pool.query(`
        UPDATE core.org_units
        SET parent_id = $1
        WHERE id = $2 AND parent_id IS NULL
      `, [gerenciaPrincipal.org_unit_id, d.org_unit_id]);
    }
  }

  // -----------------------------------------------------------------
  // FIN
  // -----------------------------------------------------------------
  return {
    ok: true,
    message: "Jerarquía normalizada correctamente",
    resumen: {
      gerencias: gerencias.length,
      direcciones: direcciones.length,
      coordinaciones: coordinaciones.length,
      asesorias: asesorias.length
    }
  };
}

// services/jerarquia.get_tree.service.js
import pool from "../config/database.js";

// Normalización ligera
function norm(s) {
  return s ? String(s).trim().toUpperCase() : "";
}

export async function getJerarquiaTree() {
  // ===============================================================
  // 1) Cargar todos los usuarios y org_units
  // ===============================================================
  const usersRes = await pool.query(`
    SELECT 
      u.id, u.name, u.document_id, u.role,
      u.org_unit_id, u.coordinator_id,
      u.district, u.regional
    FROM core.users u
    ORDER BY u.role, u.name
  `);

  const unitsRes = await pool.query(`
    SELECT id, name, unit_type, parent_id
    FROM core.org_units
    ORDER BY id
  `);

  const users = usersRes.rows;
  const units = unitsRes.rows;

  // ===============================================================
  // 2) Indexación rápida
  // ===============================================================
  const mapUserByUnit = {};
  const mapUserById = {};
  const mapUnit = {};

  for (const u of users) {
    mapUserById[u.id] = u;
    if (!mapUserByUnit[u.org_unit_id]) mapUserByUnit[u.org_unit_id] = [];
    mapUserByUnit[u.org_unit_id].push(u);
  }

  for (const unit of units) {
    mapUnit[unit.id] = unit;
  }

  // ===============================================================
  // 3) Encontrar GERENTES (unit_type = GERENCIA)
  // ===============================================================
  const gerencias = units.filter(u => u.unit_type === "GERENCIA");

  const tree = [];

  for (const gUnit of gerencias) {
    const gerente = (mapUserByUnit[gUnit.id] || []).find(u => u.role === "GERENCIA");

    const nodoGerente = {
      id: gerente?.id || null,
      nombre: gerente?.name || gUnit.name,
      cedula: gerente?.document_id || null,
      distrito: gerente?.district || gUnit.name,
      unit_id: gUnit.id,
      directores: []
    };

    // ===========================================================
    // 4) Directores cuyo parent_id = gUnit.id
    // ===========================================================
    const directoresUnits = units.filter(u => u.parent_id === gUnit.id && u.unit_type === "DIRECCION");

    for (const dUnit of directoresUnits) {
      const director = (mapUserByUnit[dUnit.id] || []).find(u => u.role === "DIRECCION");

      const nodoDirector = {
        id: director?.id || null,
        nombre: director?.name || dUnit.name,
        cedula: director?.document_id || null,
        distrito: director?.district || dUnit.name,
        unit_id: dUnit.id,
        coordinadores: []
      };

      // ===========================================================
      // 5) Coordinadores (parent = director)
      // ===========================================================
      const coordUnits = units.filter(u => u.parent_id === dUnit.id && u.unit_type === "COORDINACION");

      for (const cUnit of coordUnits) {
        const coord = (mapUserByUnit[cUnit.id] || []).find(u => u.role === "COORDINACION");

        const nodoCoord = {
          id: coord?.id || null,
          nombre: coord?.name || cUnit.name,
          cedula: coord?.document_id || null,
          distrito: coord?.district || cUnit.name,
          unit_id: cUnit.id,
          asesores: []
        };

        // ===========================================================
        // 6) Asesores supervisados por este coordinador
        // ===========================================================
        const asesores = users.filter(u => u.role === "ASESORIA" && u.coordinator_id === coord?.id);

        nodoCoord.asesores = asesores.map(a => ({
          id: a.id,
          nombre: a.name,
          cedula: a.document_id,
          distrito: a.district
        }));

        nodoDirector.coordinadores.push(nodoCoord);
      }

      nodoGerente.directores.push(nodoDirector);
    }

    tree.push(nodoGerente);
  }

  return {
    ok: true,
    total_gerentes: tree.length,
    arbol: tree
  };
}

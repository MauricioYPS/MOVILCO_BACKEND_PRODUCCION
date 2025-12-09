/********************************************************************************************
 * NOVEDADES GERENCIALES — Vista Gerencial (2025-12) — FIXED
 ********************************************************************************************/
import pool from "../../config/database.js";
import {
  loadDistrictMap,
  normalizeDistrict
} from "../../services/kpi.calculate.service.js";

/********************************************************************************************
 * HELPERS
 ********************************************************************************************/
function parsePeriod(period) {
  const m = String(period).match(/^(\d{4})-(\d{2})$/);
  if (!m) return null;
  return { year: Number(m[1]), month: Number(m[2]) };
}

/********************************************************************************************
 * CARGAR NOVEDADES (user_id, NO cedula)
 ********************************************************************************************/
async function loadNovedades(period) {
  const { year, month } = period;

  const q = `
    SELECT 
      id,
      user_id,
      tipo,
      descripcion,
      fecha_inicio,
      fecha_fin
    FROM kpi.novedades
    WHERE 
      (EXTRACT(YEAR FROM fecha_inicio) = $1 AND EXTRACT(MONTH FROM fecha_inicio) = $2)
      OR
      (EXTRACT(YEAR FROM fecha_fin) = $1 AND EXTRACT(MONTH FROM fecha_fin) = $2)
    ORDER BY fecha_inicio ASC
  `;

  const { rows } = await pool.query(q, [year, month]);
  return rows;
}

/********************************************************************************************
 * CARGAR USUARIOS (id → usuario)
 ********************************************************************************************/
async function loadUsers() {
  const q = `
    SELECT 
      id,
      document_id AS cedula,
      name,
      district,
      district_claro,
      org_unit_id
    FROM core.users
  `;

  const { rows } = await pool.query(q);

  const map = {};
  for (const u of rows) {
    map[u.id] = u; // indexar por user_id, NO por cedula
  }
  return map;
}

/********************************************************************************************
 * Cargar Unidades Organizacionales
 ********************************************************************************************/
async function loadOrgUnits() {
  const q = `SELECT id, name, unit_type, parent_id FROM core.org_units`;
  const { rows } = await pool.query(q);

  const map = {};
  for (const u of rows) map[u.id] = u;
  return map;
}

/********************************************************************************************
 * ENDPOINT PRINCIPAL — FIXED
 ********************************************************************************************/
export async function getAllNovedadesGerenciales(req, res) {
  try {
    const period = parsePeriod(req.query.period);

    if (!period) {
      return res.status(400).json({
        ok: false,
        error: "Periodo inválido. Use YYYY-MM"
      });
    }

    /**********************************************
     * 1. Cargar dataset (TODO en paralelo)
     **********************************************/
    const [novedades, usersMap, orgUnits, districtMap] = await Promise.all([
      loadNovedades(period),
      loadUsers(),
      loadOrgUnits(),
      loadDistrictMap()
    ]);

    /**********************************************
     * 2. AGRUPAR POR DISTRITO
     **********************************************/
    const distritos = {};

    for (const n of novedades) {

      const user = usersMap[n.user_id]; // ← FIX: enlazar por user_id

      if (!user) continue;

      const distrito = normalizeDistrict(
        user.district_claro || user.district,
        districtMap
      ) || "SIN DISTRITO";

      const coord = orgUnits[user.org_unit_id];
      const direccion = coord ? orgUnits[coord.parent_id] : null;

      if (!distritos[distrito]) distritos[distrito] = [];

      distritos[distrito].push({
        distrito,
        tipo: n.tipo,
        descripcion: n.descripcion,
        fecha_inicio: n.fecha_inicio,
        fecha_fin: n.fecha_fin,
        asesor_nombre: user.name,
        asesor_cedula: user.cedula,   // ← FIX: ahora sí trae la cédula
        coordinacion: coord ? coord.name : null,
        direccion: direccion ? direccion.name : null
      });
    }

    /**********************************************
     * 3. Respuesta final
     **********************************************/
    return res.json({
      ok: true,
      periodo: req.query.period,
      total_distritos: Object.keys(distritos).length,
      resultado: distritos
    });

  } catch (err) {
    console.error("[getAllNovedadesGerenciales] error:", err);
    return res.status(500).json({
      ok: false,
      error: "Error interno",
      detail: err.message
    });
  }
}

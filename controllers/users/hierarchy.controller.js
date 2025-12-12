/********************************************************************************************
 * HIERARCHY CONTROLLER — Versión Final Definitiva (2025-12-08)
 ********************************************************************************************/
import pool from "../../config/database.js";
import { getKpiResume } from "../../services/kpi.resume.service.js";

/********************************************************************************************
 * HELPERS
 ********************************************************************************************/
function cleanId(raw) {
  return Number(String(raw || "").trim());
}

async function loadOrgUnit(id) {
  const q = `
    SELECT id, name, unit_type, parent_id
    FROM core.org_units
    WHERE id = $1
  `;
  const { rows } = await pool.query(q, [id]);
  return rows[0] || null;
}

/** Novedades por periodo */
async function loadNovedades(userId, period) {
  const [year, month] = period.split("-").map(Number);
  const q = `
      SELECT *
      FROM kpi.novedades
      WHERE user_id = $1
        AND (
              (EXTRACT(YEAR FROM fecha_inicio) = $2 AND EXTRACT(MONTH FROM fecha_inicio) = $3)
           OR (EXTRACT(YEAR FROM fecha_fin) = $2 AND EXTRACT(MONTH FROM fecha_fin) = $3)
        )
  `;
  const { rows } = await pool.query(q, [userId, year, month]);
  return rows;
}
async function loadCoordinatorUser(coordUnitId) {
  const q = `
    SELECT id, name, document_id, active
    FROM core.users
    WHERE org_unit_id = $1
    ORDER BY created_at ASC
    LIMIT 1
  `;
  const { rows } = await pool.query(q, [coordUnitId]);
  return rows[0] || null;
}


/********************************************************************************************
 * 1. ASESOR POR COORDINADOR
 ********************************************************************************************/
async function loadAsesoresByCoord(coordUnitId) {
  const q = `
      SELECT id, name, document_id, org_unit_id, active,
             district_claro, district
      FROM core.users
      WHERE org_unit_id = $1
      ORDER BY name ASC
  `;
  const { rows } = await pool.query(q, [coordUnitId]);
  return rows;
}

export async function getAsesoresByCoordinador(req, res) {
  try {
    const coordId = cleanId(req.params.id);
    const period = req.query.period;

    const coordinador = await pool.query(`
  SELECT 
    id, 
    name,
    document_id,
    phone,
    email,
    district_claro,
    district,
    active,
    org_unit_id,
    created_at,
    updated_at
  FROM core.users
  WHERE id = $1
`, [coordId]).then(r => r.rows[0]);

    if (!coordinador)
      return res.status(404).json({ ok: false, error: "Coordinador no encontrado" });

    const asesores = await loadAsesoresByCoord(coordinador.org_unit_id);

    const resultado = [];
    for (const a of asesores) {
      const resumen = await getKpiResume(a, period);
      const novedades = await loadNovedades(a.id, period);

      resultado.push({
        ...a,
        ventas: resumen.ventas,
        ventas_distrito: resumen.ventas_distrito,
        ventas_fuera: resumen.ventas_fuera,
        dias_laborados: resumen.dias_laborados,
        prorrateo: resumen.prorrateo,
        novedades
      });
    }

    return res.json({
      ok: true,
      coordinador,
      periodo: period,
      total: resultado.length,
      asesores: resultado
    });

  } catch (err) {
    console.error("[getAsesoresByCoordinador]", err);
    return res.status(500).json({ ok: false, error: "Error interno" });
  }
}

/********************************************************************************************
 * 2. COORDINADORES POR DIRECCIÓN
 ********************************************************************************************/
async function loadCoordUnitsByDirection(directionId) {
  const q = `
    SELECT id, name, unit_type
    FROM core.org_units
    WHERE parent_id = $1
      AND unit_type = 'COORDINACION'
    ORDER BY name ASC
  `;
  const { rows } = await pool.query(q, [directionId]);
  return rows;
}

export async function getCoordinadoresByDireccion(req, res) {
  try {
    const directionId = cleanId(req.params.id);
    const period = req.query.period;

    // 1. Cargar la dirección
    const direccion = await loadOrgUnit(directionId);
    if (!direccion) {
      return res.status(404).json({
        ok: false,
        error: "Dirección no encontrada",
      });
    }

    // 2. Coordinaciones bajo la dirección
    const coordUnits = await loadCoordUnitsByDirection(directionId);
    if (!coordUnits.length) {
      return res.json({
        ok: true,
        direccion,
        periodo: period,
        total: 0,
        coordinadores: [],
      });
    }

    // 3. Cargar asesores por coordinación
    const advisorsByCoord = await Promise.all(
      coordUnits.map((cu) => loadAsesoresByCoord(cu.id))
    );

    // 4. Aplanar asesores agregando referencia de la coordinación
    const asesoresExtendidos = [];
    for (let i = 0; i < coordUnits.length; i++) {
      const cu = coordUnits[i];
      const asesores = advisorsByCoord[i];

      for (const a of asesores) {
        asesoresExtendidos.push({
          ...a,
          coord_id: cu.id,
          coord_name: cu.name,
          coord_unit_type: cu.unit_type,
        });
      }
    }

    // Si no hay asesores, retornar coordinaciones con totales 0
    if (!asesoresExtendidos.length) {
      return res.json({
        ok: true,
        direccion,
        periodo: period,
        total: coordUnits.length,
        coordinadores: coordUnits.map((cu) => ({
          coord_unit_id: cu.id,
          id: null,
          coordinator_name: null,
          name: cu.name,
          unit_type: cu.unit_type,
          total_asesores: 0,
          total_ventas: 0,
          ventas_distrito: 0,
          ventas_fuera: 0,
        })),
      });
    }

    // 5. KPI en paralelo
    const resumes = await Promise.all(
      asesoresExtendidos.map((a) => getKpiResume(a, period))
    );

    // 6. Agregación por coordinación
    const map = Object.create(null);

    for (let i = 0; i < asesoresExtendidos.length; i++) {
      const a = asesoresExtendidos[i];
      const resumen = resumes[i] || {};

      let bucket = map[a.coord_id];
      if (!bucket) {

        // Cargar coordinador real de esa coordinación
        const coordinatorUser = await loadCoordinatorUser(a.coord_id);

        bucket = map[a.coord_id] = {
          coord_unit_id: a.coord_id, // ID org unit
          id: coordinatorUser ? coordinatorUser.id : null, // ← ID REAL DEL COORDINADOR
          coordinator_name: coordinatorUser ? coordinatorUser.name : null,

          name: a.coord_name, 
          unit_type: a.coord_unit_type,

          total_asesores: 0,
          total_ventas: 0,
          ventas_distrito: 0,
          ventas_fuera: 0,
        };
      }

      bucket.total_asesores++;
      bucket.total_ventas += resumen.ventas || 0;
      bucket.ventas_distrito += resumen.ventas_distrito || 0;
      bucket.ventas_fuera += resumen.ventas_fuera || 0;
    }

    // Convertir mapa a array ordenado
    const coordinadores = Object.values(map).sort((a, b) =>
      a.name.localeCompare(b.name, "es")
    );

    return res.json({
      ok: true,
      direccion,
      periodo: period,
      total: coordinadores.length,
      coordinadores,
    });

  } catch (err) {
    console.error("[getCoordinadoresByDireccion]", err);
    return res.status(500).json({
      ok: false,
      error: "Error interno",
      detail: err.message,
    });
  }
}




/********************************************************************************************
 * 3. DIRECCIONES POR GERENCIA
 ********************************************************************************************/
async function loadDirectionsByGerencia(parentId) {
  const q = `
    SELECT id, name, unit_type
    FROM core.org_units
    WHERE parent_id = $1
      AND unit_type = 'DIRECCION'
    ORDER BY name ASC
  `;
  const { rows } = await pool.query(q, [parentId]);
  return rows;
}

export async function getDireccionesByGerencia(req, res) {
  try {
    let id = cleanId(req.params.id);

    let gerencia = await loadOrgUnit(id);

    /** Fallback: si id es usuario y no org unit */
    if (!gerencia) {
      const q = `
        SELECT ou.id, ou.name, ou.unit_type, ou.parent_id
        FROM core.users u
        JOIN core.org_units ou ON ou.id = u.org_unit_id
        WHERE u.id = $1
      `;
      gerencia = (await pool.query(q, [id])).rows[0] || null;
    }

    if (!gerencia)
      return res.status(404).json({ ok: false, error: "Gerencia no encontrada" });

    const direcciones = await loadDirectionsByGerencia(gerencia.id);

    return res.json({
      ok: true,
      gerencia,
      total: direcciones.length,
      direcciones
    });

  } catch (err) {
    console.error("[getDireccionesByGerencia]", err);
    return res.status(500).json({ ok: false, error: "Error interno" });
  }
}


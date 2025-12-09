/********************************************************************************************
 * V2 REAL — Basado en FULL_SALES + GENERATED_SALES (misma lógica que getKpiResume)
 ********************************************************************************************/
import pool from "../../config/database.js";
import {
  loadAllSalesForPeriod,
  loadDistrictMap,
  extractAsesorCedula,
  computeSalesForAsesor,
} from "../../services/kpi.calculate.service.js";

function parsePeriod(period) {
  const match = String(period).match(/^(\d{4})-(\d{2})$/);
  if (!match) return null;
  return { year: Number(match[1]), month: Number(match[2]) };
}

async function loadOrgUnit(id) {
  const q = `SELECT id, name, unit_type, parent_id FROM core.org_units WHERE id=$1`;
  const { rows } = await pool.query(q, [id]);
  return rows[0] || null;
}

export async function getCoordinadoresByDireccionV2(req, res) {
  try {
    const directionId = Number(req.params.direction_id);
    const period = req.query.period;

    const per = parsePeriod(period);
    if (!per) {
      return res.status(400).json({ ok: false, error: "Periodo inválido" });
    }

    const { year, month } = per;

    const direccion = await loadOrgUnit(directionId);
    if (!direccion || direccion.unit_type !== "DIRECCION") {
      return res.status(404).json({ ok: false, error: "Dirección no encontrada" });
    }

    /*************************************************************
     * 1. Obtener coordinaciones bajo esta dirección
     *************************************************************/
    const coordQ = `
      SELECT id, name, unit_type
      FROM core.org_units
      WHERE parent_id = $1 AND unit_type = 'COORDINACION'
    `;
    const { rows: coordinaciones } = await pool.query(coordQ, [directionId]);

    if (coordinaciones.length === 0) {
      return res.json({
        ok: true,
        direccion,
        periodo: period,
        total: 0,
        coordinadores: []
      });
    }

    /*************************************************************
     * 2. Obtener asesores bajo cada coordinación
     *************************************************************/
    const asesoresQ = `
      SELECT id, document_id, name, district_claro, district, org_unit_id
      FROM core.users
      WHERE org_unit_id = ANY($1)
    `;

    const coordIds = coordinaciones.map(c => c.id);
    const { rows: asesores } = await pool.query(asesoresQ, [coordIds]);

    /*************************************************************
     * 3. Cargar TODAS las ventas del periodo (full_sales + generated_sales)
     *************************************************************/
    const ventas = await loadAllSalesForPeriod({ year, month });
    const districtMap = await loadDistrictMap();

    /*************************************************************
     * 4. Organizar ventas por asesor_id (document_id)
     *************************************************************/
    const ventasPorAsesor = {};
    for (const v of ventas) {
      const cedula = extractAsesorCedula(v);
      if (!cedula) continue;
      if (!ventasPorAsesor[cedula]) ventasPorAsesor[cedula] = [];
      ventasPorAsesor[cedula].push(v);
    }

    /*************************************************************
     * 5. Agregar por coordinación
     *************************************************************/
    const mapCoord = {};

    for (const c of coordinaciones) {
      mapCoord[c.id] = {
        coord_id: c.id,
        name: c.name,
        unit_type: c.unit_type,
        total_asesores: 0,
        total_ventas: 0,
        ventas_distrito: 0,
        ventas_fuera: 0
      };
    }

    /*************************************************************
     * 6. Calcular ventas por asesor y sumar por coordinación
     *************************************************************/
    for (const a of asesores) {
      const ced = String(a.document_id).trim();
      const rows = ventasPorAsesor[ced] || [];

      const resumen = computeSalesForAsesor({
        rows,
        asesor: a,
        districtMap
      });

      const target = mapCoord[a.org_unit_id];
      target.total_asesores += 1;
      target.total_ventas += resumen.ventasTotales;
      target.ventas_distrito += resumen.ventasDistrito;
      target.ventas_fuera += resumen.ventasFuera;
    }

    /*************************************************************
     * 7. Respuesta final
     *************************************************************/
    const out = Object.values(mapCoord).sort((a, b) => a.name.localeCompare(b.name));

    return res.json({
      ok: true,
      direccion,
      periodo: period,
      total: out.length,
      coordinadores: out
    });

  } catch (err) {
    console.error("[getCoordinadoresByDireccionV2]", err);
    return res.status(500).json({ ok: false, error: "Error interno", detail: err.message });
  }
}



import pool from "../config/database.js";
import { parseSiappDate } from "../utils/parse-date-siapp.js";

/**
 * KPI GET — versión final corregida para mostrar:
 *  - TODAS las ventas del SIAPP (full + generated)
 *  - Todos los campos del detalle
 *  - Ventas manuales creadas desde tu sistema
 */
export async function getKpiForPeriod(period, filters = {}) {

  // ----------------------------
  // VALIDAR PERIODO
  // ----------------------------
  const match = String(period).match(/^(\d{4})-(\d{2})$/);
  if (!match) throw new Error("Periodo inválido. Use YYYY-MM");

  const period_year = Number(match[1]);
  const period_month = Number(match[2]);

  // ----------------------------
  // FILTROS
  // ----------------------------
  const {
    date,
    details = true,
    director_id,
    regional_id,
    coordinator_id,
    district_id,
    documento,
    distrito
  } = filters;

  let detailsFlag = !["false", "0", false, 0].includes(details);

  // FILTRO REAL SIAPP
  let dateFilter = null;
  if (date) {
    const dm = String(date).match(/^(\d{4})-(\d{2})$/);
    if (dm) dateFilter = { y: Number(dm[1]), m: Number(dm[2]) };
  }

  // ============================================================
  // 1) CARGAR KPI ENCABEZADO
  // ============================================================

  const baseQuery = `
    SELECT 
      kr.*,
      u.name AS asesor_nombre,
      cu.name AS coordinador_nombre,
      ou_dir.name AS direccion,
      ou_reg.name AS regional
    FROM kpi.kpi_resultados kr
    JOIN core.users u               ON u.id = kr.asesor_id
    LEFT JOIN core.users cu         ON cu.id = u.coordinator_id
    LEFT JOIN core.org_units ou_dis ON ou_dis.id = u.org_unit_id
    LEFT JOIN core.org_units ou_dir ON ou_dir.id = ou_dis.parent_id
    LEFT JOIN core.org_units ou_reg ON ou_reg.id = ou_dir.parent_id
    WHERE kr.period_year = $1 AND kr.period_month = $2
  `;

  const params = [period_year, period_month];
  let where = [];
  let idx = 3;

  if (documento) { where.push(`kr.documento = $${idx++}`); params.push(documento); }
  if (distrito) { where.push(`kr.distrito_claro ILIKE $${idx++}`); params.push(`%${distrito}%`); }
  if (district_id) { where.push(`kr.org_unit_id = $${idx++}`); params.push(district_id); }
  if (coordinator_id) { where.push(`u.coordinator_id = $${idx++}`); params.push(coordinator_id); }
  if (director_id) { where.push(`ou_dir.id = $${idx++}`); params.push(director_id); }
  if (regional_id) { where.push(`ou_reg.id = $${idx++}`); params.push(regional_id); }

  const finalQuery = `
    ${baseQuery}
    ${where.length ? " AND " + where.join(" AND ") : ""}
    ORDER BY kr.documento ASC
  `;

  const { rows: asesores } = await pool.query(finalQuery, params);

  if (asesores.length === 0) {
    return {
      ok: true,
      total: 0,
      resumen: { asesores: 0, total_ventas: 0 },
      data: []
    };
  }

  // ============================================================
  // 2) TRAER DETALLE COMPLETO DESDE kpi_resultados_detalle
  // ============================================================

  const kpiIds = asesores.map(a => a.id);

  const detQuery = `
    SELECT *
    FROM kpi.kpi_resultados_detalle
    WHERE kpi_id = ANY($1)
    ORDER BY kpi_id ASC, fecha ASC, id ASC
  `;

  const { rows: detalles } = await pool.query(detQuery, [kpiIds]);

  // Agrupar ventas por asesor
  const map = {};
  asesores.forEach(a => map[a.id] = []);
  detalles.forEach(v => map[v.kpi_id].push(v));

  // ============================================================
  // 3) RECALCULAR KPI Y AGREGAR DETALLE COMPLETO
  // ============================================================

  let totalVentasGlobal = 0;

  for (const a of asesores) {
    const lista = map[a.id] || [];

    let ventasTotales = 0;
    let ventasDistrito = 0;
    let ventasFuera = 0;

    const ventasDetalle = [];

    for (const v of lista) {

      // FILTRO FECHA SIAPP
      if (dateFilter) {
        const parsed = parseSiappDate(v.fecha);
        if (!parsed.date) continue;
        if (
          parsed.date.getFullYear() !== dateFilter.y ||
          parsed.date.getMonth() + 1 !== dateFilter.m
        ) continue;
      }

      ventasTotales++;

      // DISTRITO (compare normalized uppercase)
      const vd = v.d_distrito?.trim().toUpperCase();
      const da = a.distrito_claro?.trim().toUpperCase();

      if (vd && da && vd === da) ventasDistrito++;
      else ventasFuera++;

      // Push detalle completo (todas columnas)
      if (detailsFlag) ventasDetalle.push(v);
    }

    totalVentasGlobal += ventasTotales;

    a.ventas_totales = ventasTotales;
    a.ventas_distrito = ventasDistrito;
    a.ventas_fuera = ventasFuera;
    a.ventas_detalle = detailsFlag ? ventasDetalle : [];
  }

  // ============================================================
  // RESPUESTA
  // ============================================================
  return {
    ok: true,
    period: `${period_year}-${String(period_month).padStart(2, "0")}`,
    total: asesores.length,
    resumen: {
      asesores: asesores.length,
      total_ventas: totalVentasGlobal
    },
    data: asesores
  };
}

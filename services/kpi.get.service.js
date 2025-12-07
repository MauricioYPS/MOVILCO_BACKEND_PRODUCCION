// services/kpi.get.service.js
import pool from "../config/database.js";
import { parseSiappDate } from "../utils/parse-date-siapp.js";

/**
 * KPI GET — versión optimizada con:
 *  - 1 sola query para ventas
 *  - recalculo completo por asesor
 *  - filtros reales por fecha (YYYY-MM)
 *  - details=true/false
 */
export async function getKpiForPeriod(period, filters = {}) {

  if (!period) throw new Error("Falta ?period=YYYY-MM");

  const match = String(period).match(/^(\d{4})-(\d{2})$/);
  if (!match) throw new Error("Periodo inválido. Usa YYYY-MM");

  const period_year = Number(match[1]);
  const period_month = Number(match[2]);

  //-------------------------------------------------------------------
  // FILTROS
  //-------------------------------------------------------------------
  const {
    date,                     // ← FILTRO REAL DE FECHA YYYY-MM
    details = true,           // ← activar/desactivar detalle
    director_id,
    regional_id,
    coordinator_id,
    district_id,
    documento,
    distrito,
  } = filters;
  // Normalizar parámetro details
  let detailsFlag = true;

  if (details === false || details === "false" || details === 0 || details === "0") {
    detailsFlag = false;
  }

  //-------------------------------------------------------------------
  // PARSEAR FILTRO DE FECHA REAL
  //-------------------------------------------------------------------
  let dateFilter = null;

  if (date) {
    const fm = String(date).match(/^(\d{4})-(\d{2})$/);
    if (fm) {
      dateFilter = {
        y: Number(fm[1]),
        m: Number(fm[2])
      };
    }
  }

  //-------------------------------------------------------------------
  // 1️⃣ CARGAR KPI ENCABEZADO (1 QUERY)
  //-------------------------------------------------------------------
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

  if (documento) {
    where.push(`kr.documento = $${idx++}`);
    params.push(String(documento));
  }

  if (distrito) {
    where.push(`kr.distrito_claro ILIKE $${idx++}`);
    params.push(`%${distrito}%`);
  }

  if (district_id) {
    where.push(`kr.org_unit_id = $${idx++}`);
    params.push(Number(district_id));
  }

  if (coordinator_id) {
    where.push(`u.coordinator_id = $${idx++}`);
    params.push(Number(coordinator_id));
  }

  if (director_id) {
    where.push(`ou_dir.id = $${idx++}`);
    params.push(Number(director_id));
  }

  if (regional_id) {
    where.push(`ou_reg.id = $${idx++}`);
    params.push(Number(regional_id));
  }

  // Construir query final
  const finalQuery = `
    ${baseQuery}
    ${where.length ? " AND " + where.join(" AND ") : ""}
    ORDER BY kr.documento ASC
  `;

  const { rows: asesores } = await pool.query(finalQuery, params);

  // Si no hay asesores, retornar vacío
  if (asesores.length === 0) {
    return {
      ok: true,
      date: date || null,
      period: `${period_year}-${String(period_month).padStart(2, "0")}`,
      total: 0,
      resumen: { asesores: 0, total_ventas: 0 },
      data: []
    };
  }

  //-------------------------------------------------------------------
  // 2️⃣ CARGAR TODAS LAS VENTAS DE LOS ASESORES (1 SOLA QUERY)
  //-------------------------------------------------------------------
  const kpiIds = asesores.map(a => a.id);

  const ventasQuery = `
    SELECT *
    FROM kpi.kpi_resultados_detalle
    WHERE kpi_id = ANY($1)
    ORDER BY kpi_id ASC, fecha ASC, id ASC
  `;

  const { rows: ventas } = await pool.query(ventasQuery, [kpiIds]);

  //-------------------------------------------------------------------
  // 3️⃣ AGRUPAR VENTAS POR ASESOR
  //-------------------------------------------------------------------
  const ventasMap = {};
  asesores.forEach(a => ventasMap[a.id] = []);

  for (const v of ventas) {
    ventasMap[v.kpi_id].push(v);
  }

  //-------------------------------------------------------------------
  // 4️⃣ RECALCULAR VENTAS POR ASESOR (con filtro real de fecha)
  //-------------------------------------------------------------------
  let totalVentasGlobal = 0;

  for (const a of asesores) {

    let ventasDistrito = 0;
    let ventasFuera = 0;
    let ventasTotales = 0;

    const detalleFinal = [];

    for (const v of ventasMap[a.id]) {

      // FILTRO DE FECHA
      if (dateFilter) {
        const parsed = parseSiappDate(v.fecha);
        if (!parsed.date) continue;

        const fy = parsed.date.getFullYear();
        const fm = parsed.date.getMonth() + 1;

        if (fy !== dateFilter.y || fm !== dateFilter.m) continue;
      }

      ventasTotales++;

      if (v.en_distrito) ventasDistrito++;
      else ventasFuera++;

      if (detailsFlag) {
        detalleFinal.push({
          venta_num: detalleFinal.length + 1,
          ...v
        });
      }
    }

    // Asignar valores recalculados
    a.ventas_distrito = ventasDistrito;
    a.ventas_fuera = ventasFuera;
    a.ventas_totales = ventasTotales;

    totalVentasGlobal += ventasTotales;

    // Asignar detalle (o vacío si details=false)
    a.ventas_detalle = detailsFlag ? detalleFinal : [];
  }

  //-------------------------------------------------------------------
  // 5️⃣ RESUMEN FINAL
  //-------------------------------------------------------------------
  const resumen = {
    asesores: asesores.length,
    total_ventas: totalVentasGlobal
  };

  return {
    ok: true,
    date: date || null,
    details: detailsFlag,
    period: `${period_year}-${String(period_month).padStart(2, "0")}`,
    total: asesores.length,
    resumen,
    data: asesores
  };
}

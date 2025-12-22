// ======================================================================
// KPI WEEKLY CALCULATE SERVICE — Versión Final 2025-12-XX (CORREGIDA + MEJORADA)
// ======================================================================

import pool from "../config/database.js";
import { loadDistrictMap, normalizeDistrict } from "./kpi.calculate.service.js";
import { getDiasLaboradosManual } from "./kpi.dias-manual.service.js";

/***********************************************************************
 * 1. UTILIDADES FECHAS — SEMANAS DOMINGO → SÁBADO
 ***********************************************************************/
function getWeeksOfMonth(year, month) {
  const weeks = [];
  const firstDay = new Date(year, month - 1, 1);
  const lastDay = new Date(year, month, 0);

  const start = new Date(firstDay);
  start.setDate(firstDay.getDate() - firstDay.getDay());

  let current = new Date(start);

  while (current <= lastDay) {
    const weekStart = new Date(current);
    const weekEnd = new Date(current);
    weekEnd.setDate(weekEnd.getDate() + 6);

    const realStart = weekStart < firstDay ? firstDay : weekStart;
    const realEnd = weekEnd > lastDay ? lastDay : weekEnd;

    weeks.push({
      start: new Date(realStart),
      end: new Date(realEnd)
    });

    current.setDate(current.getDate() + 7);
  }

  return weeks;
}

/***********************************************************************
 * 2. CARGAR USUARIOS ACTIVOS
 ***********************************************************************/
async function loadUsers() {
  const q = `
    SELECT id, document_id, name, district_claro, district,
           org_unit_id, active AS status
    FROM core.users
  `;
  const { rows } = await pool.query(q);

  const map = {};
  for (const u of rows) {
    if (u.document_id) map[String(u.document_id).trim()] = u;
  }

  return { list: rows, map };
}

/***********************************************************************
 * 3. CARGAR NOVEDADES PARA TODAS LAS SEMANAS
 ***********************************************************************/
async function loadNovedades() {
  const q = `SELECT * FROM kpi.novedades`;
  const { rows } = await pool.query(q);
  return rows;
}

function novedadesEnSemanaParaUsuario(novedades, userId, start, end) {
  return novedades.filter(n =>
    n.user_id === userId &&
    new Date(n.fecha_inicio) <= end &&
    new Date(n.fecha_fin) >= start
  );
}

/***********************************************************************
 * 4. CARGAR VENTAS DENTRO DE UNA SEMANA
 *  Incluye full_sales + generated_sales
 ***********************************************************************/
async function loadSalesWeek(weekStart, weekEnd) {

  const q = `
    -- ============================
    -- FULL SALES (archivos importados)
    -- ============================
    SELECT
      idasesor, nombreasesor, division, area, zona, poblacion,
      d_distrito, fecha, venta,
      estado_liquidacion, linea_negocio, cuenta, ot, cantserv,
      tipored, estrato, paquete_pvd, mintic,
      tipo_prodcuto, ventaconvergente, venta_instale_dth,
      sac_final, cedula_vendedor, nombre_vendedor,
      modalidad_venta, tipo_vendedor, tipo_red_comercial,
      nombre_regional, nombre_comercial, nombre_lider,
      retencion_control, observ_retencion,
      tipo_contrato, tarifa_venta, comision_neta,
      punto_equilibrio, renta, tipo_registro,
      'imported' AS source
    FROM siapp.full_sales
    WHERE fecha BETWEEN $1 AND $2

    UNION ALL

    -- ============================
    -- GENERATED SALES (creadas en Movilco)
    -- ============================
    SELECT
      idasesor, nombreasesor, division, area, zona, poblacion,
      d_distrito, fecha, venta,
      estado_liquidacion, linea_negocio, cuenta, ot, cantserv,
      tipored, estrato, paquete_pvd, mintic,
      tipo_prodcuto, ventaconvergente, venta_instale_dth,
      sac_final, cedula_vendedor, nombre_vendedor,
      modalidad_venta, tipo_vendedor, tipo_red_comercial,
      nombre_regional, nombre_comercial, nombre_lider,
      retencion_control, observ_retencion,
      tipo_contrato, tarifa_venta, comision_neta,
      punto_equilibrio, renta, tipo_registro,
      'manual' AS source
    FROM siapp.generated_sales
    WHERE fecha BETWEEN $1 AND $2
  `;

  const { rows } = await pool.query(q, [weekStart, weekEnd]);
  return rows;
}

/***********************************************************************
 * 5. EXTRAER CÉDULA DEL SIAPP
 ***********************************************************************/
function extractCedula(r) {
  if (r.idasesor) return String(r.idasesor).trim();
  return null;
}

/***********************************************************************
 * 6. CALCULAR KPI SEMANAL (VERSIÓN FINAL)
 ***********************************************************************/
export async function calculateWeeklyKpi(period) {
  if (!period.match(/^\d{4}-\d{2}$/))
    throw new Error("Periodo inválido. Usa YYYY-MM");

  const [year, month] = period.split("-").map(Number);

  const weeks = getWeeksOfMonth(year, month);

  const [usersData, novedades, districtMap] = await Promise.all([
    loadUsers(),
    loadNovedades(),
    loadDistrictMap()
  ]);

  const users = usersData.list;
  const usersMap = usersData.map;

  const results = [];

  for (let index = 0; index < weeks.length; index++) {
    const w = weeks[index];

    // FIX fundamental → Ahora el servicio recibe bien la semana
    const salesWeek = await loadSalesWeek(w.start, w.end);

    // Agrupar ventas por asesor
    const ventasPorAsesor = {};
    for (const row of salesWeek) {
      const ced = extractCedula(row);
      if (!ced) continue;
      if (!usersMap[ced]) continue;

      if (!ventasPorAsesor[ced]) ventasPorAsesor[ced] = [];
      ventasPorAsesor[ced].push(row);
    }

    const week_number = index + 1;

    for (const user of users) {
      const ced = String(user.document_id || "").trim();
      const ventas = ventasPorAsesor[ced] || [];

      // NOVEDADES SOLO DEL USUARIO CORRESPONDIENTE ✔
      const novedadesUser = novedadesEnSemanaParaUsuario(
        novedades, user.id, w.start, w.end
      );

      // Contar ventas
      let vd = 0, vf = 0;

      const distritoAsesor = normalizeDistrict(
        user.district_claro || user.district,
        districtMap
      );

      for (const v of ventas) {
        const distVenta = normalizeDistrict(v.d_distrito, districtMap);
        if (distVenta === distritoAsesor) vd++;
        else vf++;
      }

      results.push({
        asesor_id: user.id,
        documento: user.document_id,
        nombre: user.name,
        estado: user.status,
        org_unit_id: user.org_unit_id,
        distrito_claro: user.district_claro || user.district,

        week_number,
        week_start: w.start,
        week_end: w.end,
        year,

        ventas_distrito: vd,
        ventas_fuera: vf,
        ventas_totales: vd + vf,

        novedades: novedadesUser
      });
    }
  }

  return { ok: true, period, results };
}

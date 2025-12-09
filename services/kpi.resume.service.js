/**********************************************************************
 * KPI RESUME SERVICE — puente para exponer lógica del KPI
 * Usa funciones internas de kpi.calculate.service.js sin duplicar código
 **********************************************************************/
import pool from "../config/database.js";

// Importamos funciones existentes desde el archivo maestro
import {
  loadAllSalesForPeriod,
  loadNovedadesForPeriod,
  extractAsesorCedula,
  calcDiasLaborados,
  calcProrrateo,
  loadDistrictMap,
  computeSalesForAsesor,
} from "./kpi.calculate.service.js";

/**********************************************************************
 * UTILS BASICOS
 **********************************************************************/
function parsePeriod(period) {
  const match = String(period).match(/^(\d{4})-(\d{2})$/);
  if (!match) return null;
  return { year: Number(match[1]), month: Number(match[2]) };
}

/**********************************************************************
 * 1. OBTENER VENTAS DEL MES
 **********************************************************************/
export async function getVentasMes(userId, period) {
  const per = parsePeriod(period);
  if (!per) throw new Error("Periodo inválido YYYY-MM");

  const { year, month } = per;

  // Usuario → cedula
  const u = await pool.query(`SELECT document_id, district_claro, district FROM core.users WHERE id = $1`, [userId]);
  if (!u.rows.length) return 0;

  const user = u.rows[0];
  const ced = String(user.document_id).trim();

  // Cargar todas las ventas del mes
  const sales = await loadAllSalesForPeriod({ year, month });

  // Filtrar ventas del asesor
  const ventasAsesor = sales.filter(v => extractAsesorCedula(v) === ced);

  return ventasAsesor.length;
}

/**********************************************************************
 * 2. OBTENER DIAS LABORADOS (con novedades)
 **********************************************************************/
export async function getDiasLaborados(userId, period) {
  const per = parsePeriod(period);
  if (!per) throw new Error("Periodo inválido YYYY-MM");

  const { year, month } = per;

  // Cédula del usuario
  const u = await pool.query(`SELECT document_id FROM core.users WHERE id=$1`, [userId]);
  if (!u.rows.length) return 0;
  const ced = String(u.rows[0].document_id).trim();

  // Novedades del mes
  const novedades = await loadNovedadesForPeriod({ year, month });

  const novedadesAsesor = novedades.filter(n => String(n.cedula).trim() === ced);

  return calcDiasLaborados({ year, month, novedades: novedadesAsesor });
}

/**********************************************************************
 * 3. OBTENER PRORRATEO
 **********************************************************************/
export async function getProrrateo(userId, period) {
  const dias = await getDiasLaborados(userId, period);

  const per = parsePeriod(period);
  const { year, month } = per;

  const totalMes = new Date(year, month, 0).getDate();

  return calcProrrateo({
    presupuesto: 13,
    diasLaborados: dias,
    totalMes
  });
}

/**********************************************************************
 * 4. DEVOLVER LISTA COMPLETA DE NOVEDADES DEL ASESOR
 **********************************************************************/
export async function getNovedades(userId, period) {
  const per = parsePeriod(period);
  if (!per) throw new Error("Periodo inválido YYYY-MM");

  const { year, month } = per;

  const u = await pool.query(`SELECT document_id FROM core.users WHERE id=$1`, [userId]);
  if (!u.rows.length) return [];

  const ced = String(u.rows[0].document_id).trim();

  const novedades = await loadNovedadesForPeriod({ year, month });

  return novedades.filter(n => String(n.cedula).trim() === ced);
}

/**********************************************************************
 * 5. OBTENER RESUMEN COMPLETO KPI PARA UN ASESOR
 **********************************************************************/
export async function getKpiResume(user, period) {
  const per = parsePeriod(period);
  const { year, month } = per;

  const sales = await loadAllSalesForPeriod({ year, month });
  const districtMap = await loadDistrictMap();
  const novedades = await getNovedades(user.id, period);
  const diasLaborados = calcDiasLaborados({ year, month, novedades });

  const prorrateo = calcProrrateo({
    presupuesto: 13,
    diasLaborados,
    totalMes: new Date(year, month, 0).getDate()
  });

  // Ventas del asesor
  const ventasAsesor = sales.filter(v => extractAsesorCedula(v) === String(user.document_id).trim());

  const resumenVentas = computeSalesForAsesor({
    rows: ventasAsesor,
    asesor: user,
    districtMap
  });

  return {
    ventas: resumenVentas.ventasTotales,
    ventas_distrito: resumenVentas.ventasDistrito,
    ventas_fuera: resumenVentas.ventasFuera,
    dias_laborados: diasLaborados,
    prorrateo,
    novedades
  };
}
export async function getCoordinadoresByDireccion(req, res) {
  return res.json({
    ok: true,
    msg: "Endpoint getCoordinadoresByDireccion aún no implementado"
  });
}

export async function getDireccionesByGerencia(req, res) {
  return res.json({
    ok: true,
    msg: "Endpoint getDireccionesByGerencia aún no implementado"
  });
}

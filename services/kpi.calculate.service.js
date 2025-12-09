/**************************************************************
 * KPI CALCULATE SERVICE — Versión final persistente (2025-12)
 **************************************************************/
import { getDiasLaboradosManual } from "./kpi.dias-manual.service.js";
import pool from "../config/database.js";
import { parseSiappDate } from "../utils/parse-date-siapp.js";

/**********************************************************************
 * 1. UTILIDADES
 **********************************************************************/

function parsePeriod(period) {
  if (!period) return null;
  const match = String(period).match(/^(\d{4})-(\d{2})$/);
  if (!match) return null;
  return {
    year: Number(match[1]),
    month: Number(match[2])
  };
}

function daysInMonth(year, month) {
  return new Date(year, month, 0).getDate();
}

export function calcDiasLaborados({ year, month, novedades }) {
  const total = daysInMonth(year, month);
  if (!novedades?.length) return total;

  let noLaborados = 0;

  for (const n of novedades) {
    if (!n.fecha_inicio || !n.fecha_fin) continue;

    const ini = new Date(n.fecha_inicio);
    const fin = new Date(n.fecha_fin);

    const periodoIni = new Date(year, month - 1, 1);
    const periodoFin = new Date(year, month - 1, total);

    const desde = ini < periodoIni ? periodoIni : ini;
    const hasta = fin > periodoFin ? periodoFin : fin;

    const dias = Math.max(0, Math.floor((hasta - desde) / 86400000) + 1);
    noLaborados += dias;
  }

  return Math.max(0, total - noLaborados);
}

export function calcProrrateo({ presupuesto = 13, diasLaborados, totalMes }) {
  if (totalMes === 0) return 0;
  return Number(((presupuesto / totalMes) * diasLaborados).toFixed(2));
}

/**********************************************************************
 * 2. CONSULTAS BD
 **********************************************************************/

/**
 * Cargar ventas del SIAPP según el origen:
 *  - simple → solo full_sales
 *  - manual → solo generated_sales
 *  - all    → vista unificada (por defecto)
 */
export async function loadAllSalesForPeriod({ year, month }) {

  const q = `
    WITH manual_sales AS (
      SELECT
        idasesor,
        nombreasesor,
        division,
        area,
        zona,
        poblacion,
        d_distrito,
        fecha,
        estado_liquidacion,
        linea_negocio,
        cuenta,
        ot,
        cantserv,
        tipored,
        estrato,
        paquete_pvd,
        mintic,
        tipo_prodcuto,
        ventaconvergente,
        venta_instale_dth,
        sac_final,
        cedula_vendedor,
        nombre_vendedor,
        modalidad_venta,
        tipo_vendedor,
        tipo_red_comercial,
        nombre_regional,
        nombre_comercial,
        nombre_lider,
        retencion_control,
        observ_retencion,
        tipo_contrato,
        tarifa_venta,
        comision_neta,
        punto_equilibrio,
        venta,
        renta,
        tipo_registro,
        period_year,
        period_month,
        'manual' AS source
      FROM siapp.generated_sales
      WHERE (
        fecha IS NOT NULL
        AND EXTRACT(YEAR FROM fecha) = $1
        AND EXTRACT(MONTH FROM fecha) = $2
      )
      OR (
        fecha IS NULL
        AND period_year = $1
        AND period_month = $2
      )
    )

    SELECT
      idasesor,
      nombreasesor,
      division,
      area,
      zona,
      poblacion,
      d_distrito,
      fecha,
      estado_liquidacion,
      linea_negocio,
      cuenta,
      ot,
      cantserv,
      tipored,
      estrato,
      paquete_pvd,
      mintic,
      tipo_prodcuto,
      ventaconvergente,
      venta_instale_dth,
      sac_final,
      cedula_vendedor,
      nombre_vendedor,
      modalidad_venta,
      tipo_vendedor,
      tipo_red_comercial,
      nombre_regional,
      nombre_comercial,
      nombre_lider,
      retencion_control,
      observ_retencion,
      tipo_contrato,
      tarifa_venta,
      comision_neta,
      punto_equilibrio,
      venta,
      renta,
      tipo_registro,
      period_year,
      period_month,
      'imported' AS source
    FROM siapp.full_sales
    WHERE (
      fecha IS NOT NULL
      AND EXTRACT(YEAR FROM fecha) = $1
      AND EXTRACT(MONTH FROM fecha) = $2
    )
    OR (
      fecha IS NULL
      AND period_year = $1
      AND period_month = $2
    )

    UNION ALL

    SELECT *
    FROM manual_sales
  `;

  const { rows } = await pool.query(q, [year, month]);
  return rows;
}








async function loadUsers() {
  const q = `
    SELECT id, document_id, name,
           district_claro, district,
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

export async function loadNovedadesForPeriod({ year, month }) {
  const q = `
    SELECT *
    FROM kpi  .novedades
    WHERE (EXTRACT(YEAR FROM fecha_inicio) = $1 AND EXTRACT(MONTH FROM fecha_inicio) = $2)
       OR (EXTRACT(YEAR FROM fecha_fin) = $1 AND EXTRACT(MONTH FROM fecha_fin) = $2)
  `;
  const { rows } = await pool.query(q, [year, month]);
  return rows;
}

export async function loadDistrictMap() {
  const q = `
    SELECT raw_name, normalized_name, official_district_name, is_official
    FROM siapp.district_map
  `;
  const { rows } = await pool.query(q);

  const map = {};
  for (const r of rows) {
    map[r.raw_name] = {
      normalized: r.normalized_name,
      official: r.official_district_name,
      isOfficial: r.is_official
    };
  }
  return map;
}

/**********************************************************************
 * 3. PERSISTENCIA KPI (ENCABEZADO + DETALLE)
 **********************************************************************/

async function clearKpiPeriod(year, month) {
  await pool.query("DELETE FROM kpi.kpi_resultados_detalle WHERE period_year=$1 AND period_month=$2", [year, month]);
  await pool.query("DELETE FROM kpi.kpi_resultados WHERE period_year=$1 AND period_month=$2", [year, month]);
}

async function saveKpiHeader(kpi, year, month) {
  const q = `
    INSERT INTO kpi.kpi_resultados (
      asesor_id, documento, nombre, estado, org_unit_id, distrito_claro,
      dias_laborados, presupuesto_prorrateado,
      ventas_distrito, ventas_fuera, ventas_totales,
      cumple_distrito, cumple_global,
      period_year, period_month
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
    RETURNING id
  `;

  const params = [
    kpi.asesor_id,
    kpi.documento,
    kpi.nombre,
    kpi.estado,
    kpi.org_unit_id,
    kpi.distrito_claro,
    kpi.dias_laborados,
    kpi.presupuesto_prorrateado,
    kpi.ventas_distrito,
    kpi.ventas_fuera,
    kpi.ventas_totales,
    kpi.cumple_distrito,
    kpi.cumple_global,
    year,
    month
  ];

  const { rows } = await pool.query(q, params);
  return rows[0].id;
}

async function saveKpiDetail(kpiId, asesor, ventas, year, month) {
  if (!ventas || ventas.length === 0) return;

  const q = `
    INSERT INTO kpi.kpi_resultados_detalle (
      kpi_id, asesor_id, documento, period_year, period_month,

      idasesor, nombreasesor, division, area, zona, poblacion, d_distrito,
      fecha, estado_liquidacion, linea_negocio, cuenta, ot, cantserv, tipored,
      estrato, paquete_pvd, mintic, tipo_prodcuto, ventaconvergente,
      venta_instale_dth, sac_final, cedula_vendedor, nombre_vendedor,
      modalidad_venta, tipo_vendedor, tipo_red_comercial, nombre_regional,
      nombre_comercial, nombre_lider, retencion_control, observ_retencion,
      tipo_contrato, tarifa_venta, comision_neta, punto_equilibrio,
      venta, renta, tipo_registro
    )
    VALUES (
      $1,$2,$3,$4,$5,
      $6,$7,$8,$9,$10,$11,$12,
      $13,$14,$15,$16,$17,$18,$19,
      $20,$21,$22,$23,$24,
      $25,$26,$27,$28,$29,$30,
      $31,$32,$33,$34,$35,$36,$37,$38,$39,$40,
      $41,$42,$43
    );
  `;

  for (const r of ventas) {
    const params = [
      kpiId,
      asesor.id,
      asesor.document_id,
      year,
      month,

      r.idasesor ?? null,
      r.nombreasesor ?? null,
      r.division ?? null,
      r.area ?? null,
      r.zona ?? null,
      r.poblacion ?? null,
      r.d_distrito ?? null,
      r.fecha ?? null,
      r.estado_liquidacion ?? null,
      r.linea_negocio ?? null,
      r.cuenta ?? null,
      r.ot ?? null,
      r.cantserv ?? null,
      r.tipored ?? null,
      r.estrato ?? null,
      r.paquete_pvd ?? null,
      r.mintic ?? null,
      r.tipo_prodcuto ?? null,
      r.ventaconvergente ?? null,
      r.venta_instale_dth ?? null,
      r.sac_final ?? null,
      r.cedula_vendedor ?? null,
      r.nombre_vendedor ?? null,
      r.modalidad_venta ?? null,
      r.tipo_vendedor ?? null,
      r.tipo_red_comercial ?? null,
      r.nombre_regional ?? null,
      r.nombre_comercial ?? null,
      r.nombre_lider ?? null,
      r.retencion_control ?? null,
      r.observ_retencion ?? null,
      r.tipo_contrato ?? null,
      r.tarifa_venta ?? null,
      r.comision_neta ?? null,
      r.punto_equilibrio ?? null,
      r.venta ?? null,
      r.renta ?? null,
      r.tipo_registro ?? null
    ];

    await pool.query(q, params);
  }
}


/**********************************************************************
 * 4. NORMALIZACIÓN
 **********************************************************************/

export function normalizeDistrict(raw, map) {
  if (!raw) return null;
  const key = String(raw).trim().toUpperCase();
  const entry = map[key];
  if (!entry) return null;
  if (entry.official) return entry.official.toUpperCase();
  return entry.normalized?.toUpperCase() || null;
}

/**********************************************************************
 * 5. EXTRAER CÉDULA SIAPP
 **********************************************************************/

/**
 * Regla oficial:
 * - Toda venta (full_sales y generated_sales) debe ser asignada al asesor usando:
 *   idasesor = core.users.document_id
 */
export function extractAsesorCedula(row) {
  if (!row) return null;

  // idasesor SIEMPRE debe existir porque lo forzamos en generated_sales
  if (row.idasesor) {
    return String(row.idasesor).trim();
  }

  // fallback para full_sales por si existe algún archivo viejo
  if (row.documento) {
    return String(row.documento).trim();
  }

  return null;
}


/**********************************************************************
 * 6. CÁLCULO VENTAS POR ASESOR
 **********************************************************************/

export function computeSalesForAsesor({ rows, asesor, districtMap }) {
  let ventasDistrito = 0;
  let ventasFuera = 0;

  const distritoAsesor = normalizeDistrict(
    asesor.district_claro || asesor.district,
    districtMap
  );

  for (const r of rows) {
    const distritoVenta = normalizeDistrict(r.d_distrito, districtMap);
    const venta = 1;

    if (distritoVenta && distritoAsesor && distritoVenta === distritoAsesor)
      ventasDistrito += venta;
    else
      ventasFuera += venta;
  }

  return {
    ventasDistrito,
    ventasFuera,
    ventasTotales: ventasDistrito + ventasFuera
  };
}

/**********************************************************************
 * 7. KPI POR ASESOR
 **********************************************************************/

function computeKpiForAsesor({ asesor, ventas, diasLaborados, prorrateo }) {
  return {
    asesor_id: asesor.id,
    documento: asesor.document_id,
    nombre: asesor.name,
    estado: asesor.status,
    org_unit_id: asesor.org_unit_id,
    distrito_claro: asesor.district_claro || asesor.district || null,

    dias_laborados: diasLaborados,
    presupuesto_prorrateado: prorrateo,

    ventas_distrito: ventas.ventasDistrito,
    ventas_fuera: ventas.ventasFuera,
    ventas_totales: ventas.ventasTotales,

    cumple_distrito: ventas.ventasDistrito >= prorrateo,
    cumple_global: ventas.ventasTotales >= prorrateo
  };
}

/**********************************************************************
 * 8. FUNCIÓN PRINCIPAL (RECALCULA + GUARDA + DEVUELVE JSON)
 **********************************************************************/

export async function calculateKpiForPeriod(period) {
  const per = parsePeriod(period);
  if (!per) throw new Error("Periodo inválido. Usa YYYY-MM");
  const { year, month } = per;


  /******************************************************
   * 1) LIMPIAR DATOS ANTERIORES
   ******************************************************/
  await clearKpiPeriod(year, month);

  /******************************************************
   * 2) CARGAR DATOS BASE
   ******************************************************/
  const [sales, usersData, novedades, districtMap] = await Promise.all([
    loadAllSalesForPeriod({ year, month }),
    loadUsers(),
    loadNovedadesForPeriod({ year, month }),
    loadDistrictMap()
  ]);

  const users = usersData.list;
  const usersMap = usersData.map;

  const novedadesPorCedula = {};
  for (const n of novedades) {
    const ced = String(n.cedula || "").trim();
    if (!ced) continue;
    if (!novedadesPorCedula[ced]) novedadesPorCedula[ced] = [];
    novedadesPorCedula[ced].push(n);
  }

  /******************************************************
   * 3) MAPEO DE VENTAS
   ******************************************************/
  /**
   * Construcción de ventas por asesor (clave → document_id)
   *
   * El KPI solo reconoce ventas si:
   *    row.idasesor === users.document_id
   */
  const ventasPorAsesor = {};

  for (const row of sales) {
    const asesorDoc = extractAsesorCedula(row);

    if (!asesorDoc) continue;

    // Si el documento no existe en core.users, no debe contarse
    const asesor = usersMap[asesorDoc];
    if (!asesor) continue;

    if (!ventasPorAsesor[asesorDoc]) {
      ventasPorAsesor[asesorDoc] = [];
    }

    ventasPorAsesor[asesorDoc].push(row);
  }


  /******************************************************
   * 4) PROCESAR CADA ASESOR
   ******************************************************/
  const totalMes = daysInMonth(year, month);
  const kpiRows = [];

  for (const u of users) {
    const cedula = String(u.document_id || "").trim();
    const ventas = ventasPorAsesor[cedula] || [];

    let diasLaborados = calcDiasLaborados({
      year,
      month,
      novedades: novedadesPorCedula[cedula] || []
    });

    // 2. Revisar si existe override manual en BD
    const manual = await getDiasLaboradosManual({
      user_id: u.id,   // o asesor.id según tu variable real
      year,
      month
    });

    // 3. Si existe manual, reemplazar
    if (manual) {
      diasLaborados = manual.dias;
    }


    const prorrateo = calcProrrateo({
      presupuesto: 13,
      diasLaborados,
      totalMes
    });

    const resumen = computeSalesForAsesor({
      rows: ventas,
      asesor: u,
      districtMap
    });

    const kpi = computeKpiForAsesor({
      asesor: u,
      ventas: resumen,
      diasLaborados,
      prorrateo
    });

    const kpiId = await saveKpiHeader(kpi, year, month);
    await saveKpiDetail(kpiId, u, ventas, year, month);

    kpiRows.push(kpi);
  }

  /******************************************************
   * 5) DEVOLVER JSON → IGUAL QUE ANTES
   ******************************************************/
  return {
    ok: true,
    periodo: { year, month },
    total_asesores: kpiRows.length,
    total_ventas_reales: sales.length,
    total_kpi_persistidos: kpiRows.length,
    kpis: kpiRows
  };
}

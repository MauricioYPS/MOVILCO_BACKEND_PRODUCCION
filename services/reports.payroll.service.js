/* ============================================================
   MOVILCO — PAYROLL SERVICE FULL 2.0
   REESTRUCTURADO Y OPTIMIZADO (B + C)
   ============================================================ */

import pool from '../config/database.js'
import ExcelJS from 'exceljs'

/* ------------------ UTILIDADES GENERALES ------------------- */

export function parsePeriod(q) {
  if (!q) return null
  const m = String(q).match(/^(\d{4})-(\d{1,2})$/)
  if (!m) return null
  return { year: Number(m[1]), month: Number(m[2]) }
}

export function normalize(v) {
  if (v == null) return ''
  return String(v)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim()
}

export function monthNameES(m) {
  const names = [
    'ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO',
    'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE'
  ]
  return names[(m - 1) % 12]
}

export function boolYesNo(v) {
  return v ? 'SI' : 'NO'
}

export const safeNum = n => (n == null || isNaN(n)) ? 0 : Number(n)
export const safeTxt = t => (t == null ? '' : String(t))
export const safeDate = d => (d ? new Date(d) : null)

/* ============================================================
   NOTA:
   Este bloque es 100% seguro. A partir del Bloque 2 empieza
   el SQL ultra optimizado e integrado SIAPP FULL.
   ============================================================ */
/* ============================================================
   🟦 BLOQUE 2 — SQL ENTERPRISE FULL (SIAPP + NOMINA + NOVEDADES)
   ============================================================ */

async function fetchReportRowsFull({ period_year, period_month, unit_id = null }) {
  const client = await pool.connect()
  try {

    /* ------------------------------------------------------------
       1️⃣ CTE SUBUNITS & SUBUSERS
       ------------------------------------------------------------ */

    const subunitsCTE = unit_id
      ? `
        WITH RECURSIVE subunits AS (
          SELECT id FROM core.org_units WHERE id = $3
          UNION ALL
          SELECT ou.id FROM core.org_units ou
          JOIN subunits s ON ou.parent_id = s.id
        ),
        subusers AS (
          SELECT id FROM core.users WHERE org_unit_id IN (SELECT id FROM subunits)
        )`
      : `
        WITH subusers AS (
          SELECT id FROM core.users
        )`

    /* ------------------------------------------------------------
       2️⃣ CTE VENTAS SIAPP FULL — ventas del mes por usuario
       ------------------------------------------------------------ */

    const siappCTE = `
    ,
    siapp_sales AS (
      SELECT
        s.user_id,
        COUNT(*) FILTER (WHERE s.in_district = true)  AS real_in_count,
        COUNT(*) FILTER (WHERE s.in_district = false) AS real_out_count,
        COUNT(*)                                       AS real_total_count
      FROM siapp.full_sales s
      WHERE s.period_year = $1
        AND s.period_month = $2
      GROUP BY s.user_id
    )`

    /* ------------------------------------------------------------
       3️⃣ NOMINA STAGING — último registro por cédula
       ------------------------------------------------------------ */

    const nominaCTE = `
    ,
    last_nomina AS (
      SELECT DISTINCT ON (a.cedula)
        a.cedula::text,
        a.presupuesto_mes::numeric(10,2) AS s_presupuesto_mes,
        a.dias_laborados::int            AS s_dias_laborados,
        a.novedad                        AS s_novedad,
        a.fecha_inicio_contrato          AS s_contract_start,
        a.fecha_fin_contrato             AS s_contract_end
      FROM staging.archivo_nomina a
      ORDER BY a.cedula, a.imported_at DESC, a.raw_row DESC
    )`

    /* ------------------------------------------------------------
       4️⃣ NOVEDADES — cruza core.user_novelties
       ------------------------------------------------------------ */

    const novedadesCTE = `
    ,
    nov_data AS (
      SELECT n.user_id,
             STRING_AGG(
               n.novelty_type || ' ' ||
               to_char(n.start_date,'YYYY-MM-DD') || '→' ||
               to_char(n.end_date,'YYYY-MM-DD'),
               ' | '
             ) AS novedades
      FROM core.user_novelties n
      WHERE (n.start_date, n.end_date)
            OVERLAPS (
              make_date($1,$2,1),
              make_date($1,$2,1) + INTERVAL '1 month - 1 day'
            )
      GROUP BY n.user_id
    )`

    /* ------------------------------------------------------------
       5️⃣ CONTRATO — calcula ACTIVO/RETIRADO por traslape real
       ------------------------------------------------------------ */

    const contratoCTE = `
    ,
    contract_overlap AS (
      SELECT
        u.id AS user_id,

        CASE
          WHEN COALESCE(u.contract_start, ln.s_contract_start) IS NULL
           AND COALESCE(u.contract_end, ln.s_contract_end) IS NULL
            THEN TRUE

          WHEN COALESCE(u.contract_start, ln.s_contract_start) IS NOT NULL
            AND COALESCE(u.contract_end, ln.s_contract_end) IS NULL
            AND COALESCE(u.contract_start, ln.s_contract_start)
                <= (make_date($1,$2,1) + INTERVAL '1 month - 1 day')
            THEN TRUE

          WHEN COALESCE(u.contract_start, ln.s_contract_start) IS NULL
            AND COALESCE(u.contract_end, ln.s_contract_end) IS NOT NULL
            AND COALESCE(u.contract_end, ln.s_contract_end)
                >= make_date($1,$2,1)
            THEN TRUE

          WHEN (COALESCE(u.contract_start, ln.s_contract_start),
                COALESCE(u.contract_end, ln.s_contract_end))
            OVERLAPS (
                make_date($1,$2,1),
                make_date($1,$2,1) + INTERVAL '1 month - 1 day'
            )
            THEN TRUE

          ELSE FALSE
        END AS contratado_real
      FROM core.users u
      LEFT JOIN last_nomina ln ON ln.cedula = u.document_id::text
    )`

    /* ------------------------------------------------------------
       6️⃣ PROGRESS FULL — resultados procesados por usuario
       ------------------------------------------------------------ */

    const progressJoin = `
    LEFT JOIN core.progress p
      ON p.user_id = u.id
     AND p.period_year = $1
     AND p.period_month = $2
    `

    /* ------------------------------------------------------------
       7️⃣ CALCULO FINAL — normaliza, redondea y prepara la fila final
       ------------------------------------------------------------ */

    const finalCTE = `
    ,
    final_calc AS (
      SELECT
        u.id AS user_id,
        u.name AS nombre_funcionario,
        u.document_id AS cedula,
        u.phone,
        u.email,
        u.district,
        u.district_claro,

        ou.name AS unidad_nombre,
        ou.unit_type AS unidad_tipo,

        /* FALLBACK PRESUPUESTO */
        COALESCE(um.presupuesto_mes, ln.s_presupuesto_mes, 13)::numeric(10,2) AS presupuesto_mes,
        COALESCE(um.dias_laborados, ln.s_dias_laborados, 30)::int             AS dias_laborados,

        /* PRORRATEO */
        ROUND(
          COALESCE(
            um.prorrateo,
            COALESCE(um.presupuesto_mes, ln.s_presupuesto_mes, 13)::numeric
            * (
              COALESCE(um.dias_laborados, ln.s_dias_laborados, 30)::numeric
              / EXTRACT(
                  DAY FROM (
                    make_date($1,$2,1) + INTERVAL '1 month - 1 day'
                  )
                )
            )
          )
        )::int AS prorrateo_calc,

        /* VENTAS SIAPP FULL */
        COALESCE(ss.real_in_count,  0)::int AS real_in_count,
        COALESCE(ss.real_out_count, 0)::int AS real_out_count,
        COALESCE(ss.real_total_count,0)::int AS real_total_count,

        /* PROGRESS (META Y CUMPLIMIENTO) */
        p.expected_count,
        p.adjusted_count,
        p.met_in_district,
        p.met_global,
        p.compliance_in_percent,
        p.compliance_global_percent,

        /* NOVEDAD FINAL */
        COALESCE(nd.novedades, ln.s_novedad, '') AS novedades,

        /* ESTADO POR TRASLAPE REAL */
        co.contratado_real

      FROM core.users u
      JOIN subusers su ON su.id = u.id
      LEFT JOIN core.org_units ou ON ou.id = u.org_unit_id
      LEFT JOIN core.user_monthly um ON um.user_id = u.id
        AND um.period_year = $1 AND um.period_month = $2
      LEFT JOIN siapp_sales ss ON ss.user_id = u.id
      LEFT JOIN last_nomina ln ON ln.cedula = u.document_id::text
      LEFT JOIN nov_data nd ON nd.user_id = u.id
      LEFT JOIN contract_overlap co ON co.user_id = u.id
      ${progressJoin}
    )`

    /* ------------------------------------------------------------
       8️⃣ CONSULTA FINAL
       ------------------------------------------------------------ */

    const sql = `
      ${subunitsCTE}
      ${siappCTE}
      ${nominaCTE}
      ${novedadesCTE}
      ${contratoCTE}
      ${finalCTE}
      SELECT *
      FROM final_calc
      ORDER BY unidad_tipo, unidad_nombre, nombre_funcionario;
    `

    const params = unit_id
      ? [period_year, period_month, unit_id]
      : [period_year, period_month]

    const { rows } = await client.query(sql, params)

    /* ------------------------
       ENSAMBLADO FINAL JSON
       ------------------------ */
    return rows.map((r, idx) => ({
      item: idx + 1,
      cedula: safeTxt(r.cedula),
      nombre_funcionario: safeTxt(r.nombre_funcionario),

      contratado_si_no: boolYesNo(r.contratado_real),
      estado: r.contratado_real ? 'ACTIVO' : 'RETIRADO',

      distrito: safeTxt(r.district),
      distrito_claro: safeTxt(r.district_claro),

      novedades: safeTxt(r.novedades),

      presupuesto_mes: safeNum(r.presupuesto_mes),
      dias_laborados_31: safeNum(r.dias_laborados),
      prorrateo_novedades: safeNum(r.prorrateo_calc),

      garantizado_para_comisionar: safeNum(r.expected_count),
      garantizado_con_novedades: safeNum(r.adjusted_count),

      ventas_distrito: safeNum(r.real_in_count),
      ventas_fuera_distrito: safeNum(r.real_out_count),
      total_ventas: safeNum(r.real_total_count),

      diferencia_en_distrito:
        safeNum(r.real_in_count) - safeNum(r.expected_count),

      diferencia_total:
        safeNum(r.real_total_count) - safeNum(r.adjusted_count),

      cumple_distrito_zonificado: r.met_in_district ? 'CUMPLE' : 'NO CUMPLE',
      cumple_global: r.met_global ? 'CUMPLE' : 'NO CUMPLE',

      unidad_tipo: safeTxt(r.unidad_tipo),
      unidad_nombre: safeTxt(r.unidad_nombre),

      compliance_in_percent: r.compliance_in_percent,
      compliance_global_percent: r.compliance_global_percent
    }))
  } finally {
    client.release()
  }
}
/* ============================================================
   🟦 BLOQUE 3 — KPI FULL OPTIMIZADO (SIAPP + PROGRESS)
   ============================================================ */

/*
  Esta versión:
  - Usa subunits recursivo (o todas las unidades si no se envía unit_id)
  - Trae KPI global de la unidad y de sus descendientes
  - Usa progress y siapp_sales integrados del mes
  - Es muy rápida: solo 1 consulta SQL grande
*/

async function fetchKpiForUnitFull({ unit_id, year, month }) {
  const client = await pool.connect()
  try {

    const sql = `
      WITH RECURSIVE subunits AS (
        SELECT id FROM core.org_units WHERE id = $1
        UNION ALL
        SELECT ou.id
        FROM core.org_units ou
        JOIN subunits s ON ou.parent_id = s.id
      ),
      subusers AS (
        SELECT id
        FROM core.users
        WHERE org_unit_id IN (SELECT id FROM subunits)
      ),
      siapp_sales AS (
        SELECT
          s.user_id,
          COUNT(*) FILTER (WHERE s.in_district = true)  AS real_in_count,
          COUNT(*) FILTER (WHERE s.in_district = false) AS real_out_count,
          COUNT(*)                                       AS real_total_count
        FROM siapp.full_sales s
        WHERE s.period_year = $2 AND s.period_month = $3
        GROUP BY s.user_id
      ),
      prog AS (
        SELECT p.*
        FROM core.progress p
        WHERE p.period_year = $2
          AND p.period_month = $3
          AND p.user_id IN (SELECT id FROM subusers)
      ),
      agg AS (
        SELECT
          (SELECT COUNT(*) FROM subusers)::int                                AS users_total,
          (SELECT COUNT(*) FROM prog)::int                                    AS evaluated,

          /* ACCESOS */
          COALESCE(SUM(COALESCE(ss.real_in_count,0)),0)::numeric(18,2)        AS real_in_sum,
          COALESCE(SUM(COALESCE(ss.real_out_count,0)),0)::numeric(18,2)       AS real_out_sum,
          COALESCE(SUM(COALESCE(ss.real_total_count,0)),0)::numeric(18,2)     AS real_total_sum,

          /* META AJUSTADA */
          COALESCE(SUM(p.adjusted_count),0)::numeric(18,2)                    AS adjusted_sum,

          /* CUMPLIMIENTO */
          COALESCE(AVG(p.compliance_global_percent),0)::numeric(6,2)          AS avg_global_percent,

          /* CUENTAS */
          COUNT(*) FILTER (WHERE p.met_in_district = true)                    AS met_in_count,
          COUNT(*) FILTER (WHERE p.met_global = true)                         AS met_global_count
        FROM prog p
        LEFT JOIN siapp_sales ss ON ss.user_id = p.user_id
      )

      SELECT
        ou.id,
        ou.name,
        ou.unit_type,
        a.*
      FROM agg a
      JOIN core.org_units ou ON ou.id = $1;
    `

    const { rows } = await client.query(sql, [unit_id, year, month])
    if (!rows[0]) return null

    const r = rows[0]
    const coverage =
      r.users_total > 0
        ? Number((r.evaluated / r.users_total) * 100).toFixed(2)
        : null

    return {
      unit_id: r.id,
      unidad: r.name,
      tipo: r.unit_type,

      users_total: Number(r.users_total),
      evaluados: Number(r.evaluated),

      cobertura_percent: coverage != null ? Number(coverage) : null,

      real_in_sum: Number(r.real_in_sum),
      real_out_sum: Number(r.real_out_sum),
      real_total_sum: Number(r.real_total_sum),

      adjusted_sum: Number(r.adjusted_sum),

      avg_global_percent:
        r.avg_global_percent != null ? Number(r.avg_global_percent) : 0,

      met_in_count: Number(r.met_in_count),
      met_global_count: Number(r.met_global_count)
    }
  } finally {
    client.release()
  }
}


/* ============================================================
   LISTADO DE KPI POR TODAS LAS UNIDADES
   ============================================================ */

async function fetchKpiRowsFull({ year, month, unit_id = null }) {
  const client = await pool.connect()
  try {
    const out = []

    /* ------------------------------------------------------------
       1) Un solo unit_id → KPI del padre + hijos
       ------------------------------------------------------------ */
    if (unit_id) {
      const { rows: children } = await client.query(
        `SELECT id FROM core.org_units WHERE parent_id = $1 ORDER BY name ASC`,
        [unit_id]
      )

      const head = await fetchKpiForUnitFull({ unit_id, year, month })
      if (head) out.push(head)

      for (const c of children) {
        const row = await fetchKpiForUnitFull({
          unit_id: c.id,
          year,
          month
        })
        if (row) out.push(row)
      }

      return out
    }

    /* ------------------------------------------------------------
       2) Sin unit_id → ordenar por jerarquía completa
       ------------------------------------------------------------ */

    const levels = ['GERENCIA', 'DIRECCION', 'COORDINACION']

    for (const lvl of levels) {
      const { rows: units } = await client.query(
        `SELECT id FROM core.org_units WHERE unit_type = $1 ORDER BY name ASC`,
        [lvl]
      )

      for (const u of units) {
        const row = await fetchKpiForUnitFull({
          unit_id: u.id,
          year,
          month
        })
        if (row) out.push(row)
      }
    }

    return out
  } finally {
    client.release()
  }
}
/* ============================================================
   🟩 BLOQUE 4 — EXCEL BUILDER FULL ENTERPRISE
   ============================================================ */

async function buildXlsxFull({ detailRows, kpiRows, period_year, period_month }) {
  const wb = new ExcelJS.Workbook()

  /* ------------------------------------------------------------
     HOJA 1 — ARCHIVO NOMINA (PRINCIPAL)
  ------------------------------------------------------------ */

  const ws = wb.addWorksheet('Archivo Nomina', {
    properties: { defaultRowHeight: 18 }
  })

  /* ----- TITULOS SUPERIORES COMPUESTOS ----- */

  ws.mergeCells('K1:N1')
  ws.getCell('K1').value = '1ER PARTE ENVIO PRESUPUESTO'
  ws.getCell('K1').alignment = { vertical: 'middle', horizontal: 'center' }
  ws.getCell('K1').font = { bold: true }

  ws.mergeCells('O1:P1')
  ws.getCell('O1').value = 'RECREO'
  ws.getCell('O1').alignment = { vertical: 'middle', horizontal: 'center' }
  ws.getCell('O1').font = { bold: true }

  ws.mergeCells('Q1:X1')
  ws.getCell('Q1').value =
    '3ER PARTE RESULTADO DE VENTAS (CRUCE REALIZA EL SISTEMA CON SIAPP)'
  ws.getCell('Q1').alignment = { vertical: 'middle', horizontal: 'center' }
  ws.getCell('Q1').font = { bold: true }

  ws.addRow([])

  /* ------------------------------------------------------------
     Columnas definidas — totalmente compatibles con legacy
  ------------------------------------------------------------ */

  const garantizadoMes =
    `GARANTIZADO AL ${monthNameES(period_month)} (CON NOVEDADES)`

  const cols = [
    { key: 'item', width: 6 },
    { key: 'cedula', width: 16 },
    { key: 'nombre_funcionario', width: 30 },
    { key: 'contratado_si_no', width: 12 },
    { key: 'distrito', width: 18 },
    { key: 'distrito_claro', width: 18 },
    { key: 'fecha_inicio_contrato', width: 18 },
    { key: 'fecha_fin_contrato', width: 18 },
    { key: 'novedades', width: 50 },

    { key: 'estado', width: 12 },
    { key: 'presupuesto_mes', width: 16 },
    { key: 'dias_laborados_31', width: 28 },
    { key: 'prorrateo_novedades', width: 28 },

    { key: 'recreo_dias_laborados_31', width: 28 },
    { key: 'garantizado_para_comisionar', width: 26 },
    { key: 'garantizado_con_novedades', width: 26 },

    { key: 'ventas_distrito', width: 18 },
    { key: 'ventas_fuera_distrito', width: 22 },
    { key: 'total_ventas', width: 16 },

    { key: 'diferencia_en_distrito', width: 20 },
    { key: 'diferencia_total', width: 18 },

    { key: 'cumple_distrito_zonificado', width: 26 },
    { key: 'cumple_global', width: 22 }
  ]

  ws.columns = cols

  /* ------------------------------------------------------------
     Headings
  ------------------------------------------------------------ */

  const headerTitles = [
    'ITEM',
    'CEDULA',
    'NOMBRE DE FUNCIONARIO',
    'CONTRATADO',
    'DISTRITO',
    'DISTRITO CLARO',
    'FECHA INICIO CONTRATO',
    'FECHA FIN CONTRATO',
    'NOVEDADES',
    'ESTADO',
    'PRESUPUESTO MES',
    'DIAS LABORADOS AL 31 (VALIDAR NOVEDADES DEL MES)',
    'PRORRATEO SEGÚN NOVEDADES PARA CUMPLIMIENTO',
    'DIAS LABORADOS AL 31 (VALIDAR NOVEDADES DEL MES)',
    'GARANTIZADO PARA COMISIONAR',
    garantizadoMes,
    'VENTAS EN DISTRITO',
    'VENTAS FUERA DEL DISTRITO',
    'TOTAL VENTAS',
    'DIFERENCIA EN DISTRITO',
    'DIFERENCIA TOTAL',
    'SI CUMPLE DISTRITO ZONIFICADO',
    'SI CUMPLE / NO CUMPLE'
  ]

  const headerRow = ws.getRow(3)
  headerTitles.forEach((t, i) => {
    const c = headerRow.getCell(i + 1)
    c.value = t
    c.font = { bold: true }
  })

  /* ------------------------------------------------------------
     Rows
  ------------------------------------------------------------ */

  for (const r of detailRows) ws.addRow(r)

  /* ------------------------------------------------------------
     Formatting
  ------------------------------------------------------------ */

  ws.eachRow((row, rowNum) => {
    if (rowNum <= 3) return

    const dateKeys = [
      'fecha_inicio_contrato',
      'fecha_fin_contrato'
    ]

    for (const key of dateKeys) {
      const idx = cols.findIndex(c => c.key === key) + 1
      const cell = row.getCell(idx)
      if (cell.value instanceof Date) {
        cell.numFmt = 'yyyy-mm-dd'
      }
    }

    const numericKeys = [
      'presupuesto_mes',
      'dias_laborados_31',
      'prorrateo_novedades',
      'recreo_dias_laborados_31',
      'garantizado_para_comisionar',
      'garantizado_con_novedades',
      'ventas_distrito',
      'ventas_fuera_distrito',
      'total_ventas',
      'diferencia_en_distrito',
      'diferencia_total'
    ]

    for (const key of numericKeys) {
      const idx = cols.findIndex(c => c.key === key) + 1
      const cell = row.getCell(idx)
      if (typeof cell.value === 'number') {
        cell.numFmt = '0'
      }
    }
  })

  /* ------------------------------------------------------------
     HOJA 2 — Resumen Unidades (KPI)
  ------------------------------------------------------------ */

  const ws2 = wb.addWorksheet('Resumen_Unidades')

  const kpiCols = [
    { header: 'Tipo Unidad', key: 'tipo', width: 14 },
    { header: 'Unidad', key: 'unidad', width: 28 },
    { header: 'Usuarios Totales', key: 'users_total', width: 16 },
    { header: 'Evaluados', key: 'evaluados', width: 12 },
    { header: 'Cobertura %', key: 'cobertura_percent', width: 14 },
    { header: 'Accesos IN', key: 'real_in_sum', width: 14 },
    { header: 'Accesos OUT', key: 'real_out_sum', width: 14 },
    { header: 'Accesos TOTAL', key: 'real_total_sum', width: 16 },
    { header: 'Meta Ajustada Total', key: 'adjusted_sum', width: 18 },
    { header: '% Cumplimiento Promedio', key: 'avg_global_percent', width: 22 },
    { header: 'Cumplidos en Distrito', key: 'met_in_count', width: 18 },
    { header: 'Cumplidos Global', key: 'met_global_count', width: 16 }
  ]

  ws2.columns = kpiCols
  ws2.getRow(1).font = { bold: true }

  for (const k of kpiRows) {
    ws2.addRow({
      tipo: k.tipo,
      unidad: k.unidad,
      users_total: k.users_total,
      evaluados: k.evaluados,
      cobertura_percent:
        k.cobertura_percent != null
          ? Number(k.cobertura_percent) / 100
          : '',
      real_in_sum: k.real_in_sum,
      real_out_sum: k.real_out_sum,
      real_total_sum: k.real_total_sum,
      adjusted_sum: k.adjusted_sum,
      avg_global_percent:
        k.avg_global_percent != null
          ? Number(k.avg_global_percent) / 100
          : '',
      met_in_count: k.met_in_count,
      met_global_count: k.met_global_count
    })
  }

  ws2.eachRow((row, rowNum) => {
    if (rowNum === 1) return
    const pctKeys = ['cobertura_percent', 'avg_global_percent']
    for (const key of pctKeys) {
      const idx = kpiCols.findIndex(c => c.key === key) + 1
      const cell = row.getCell(idx)
      if (typeof cell.value === 'number') {
        cell.numFmt = '0.00%'
      }
    }
  })

  /* ------------------------------------------------------------
     Retorno del buffer final
  ------------------------------------------------------------ */

  const buf = await wb.xlsx.writeBuffer()
  return buf
}
/* ============================================================
   🟧 BLOQUE 5 — CSV BUILDER FULL (ENTERPRISE EDITION)
   ============================================================ */

async function buildCsvFull({ detailRows, period, period_year, period_month }) {
  
  /* Nombre dinámico del garantizado */
  const garantizadoMes =
    `GARANTIZADO AL ${monthNameES(period_month)} (CON NOVEDADES)`

  /* Columnas oficiales en orden */
  const headers = [
    'ITEM',
    'CEDULA',
    'NOMBRE DE FUNCIONARIO',
    'CONTRATADO',
    'DISTRITO',
    'DISTRITO CLARO',
    'FECHA INICIO CONTRATO',
    'FECHA FIN CONTRATO',
    'NOVEDADES',
    'ESTADO',
    'PRESUPUESTO MES',
    'DIAS LABORADOS AL 31 (VALIDAR NOVEDADES DEL MES)',
    'PRORRATEO SEGÚN NOVEDADES PARA CUMPLIMIENTO',
    'DIAS LABORADOS AL 31 (VALIDAR NOVEDADES DEL MES)',
    'GARANTIZADO PARA COMISIONAR',
    garantizadoMes,
    'VENTAS EN DISTRITO',
    'VENTAS FUERA DEL DISTRITO',
    'TOTAL VENTAS',
    'DIFERENCIA EN DISTRITO',
    'DIFERENCIA TOTAL',
    'SI CUMPLE DISTRITO ZONIFICADO',
    'SI CUMPLE / NO CUMPLE'
  ]

  const lines = []

  /* Encabezado CSV */
  lines.push(headers.join(','))

  /* ------------------------------------------------------------
     Filas
     ------------------------------------------------------------ */
  for (const r of detailRows) {
    const row = [
      r.item,
      r.cedula,
      `"${(r.nombre_funcionario || '').replace(/"/g, '""')}"`,
      r.contratado_si_no,
      r.distrito,
      r.distrito_claro,

      r.fecha_inicio_contrato
        ? new Date(r.fecha_inicio_contrato).toISOString().slice(0, 10)
        : '',

      r.fecha_fin_contrato
        ? new Date(r.fecha_fin_contrato).toISOString().slice(0, 10)
        : '',

      `"${(r.novedades || '').replace(/"/g, '""')}"`,

      r.estado,
      r.presupuesto_mes ?? '',
      r.dias_laborados_31 ?? '',
      r.prorrateo_novedades ?? '',
      r.recreo_dias_laborados_31 ?? '',
      r.garantizado_para_comisionar ?? '',
      r.garantizado_con_novedades ?? '',

      r.ventas_distrito ?? '',
      r.ventas_fuera_distrito ?? '',
      r.total_ventas ?? '',

      r.diferencia_en_distrito ?? '',
      r.diferencia_total ?? '',

      r.cumple_distrito_zonificado,
      r.cumple_global
    ]

    lines.push(row.join(','))
  }

  /* ------------------------------------------------------------
     Resultado final
     ------------------------------------------------------------ */

  return {
    mime: 'text/csv; charset=utf-8',
    filename: `nomina_${period}.csv`,
    buffer: Buffer.from(lines.join('\n'), 'utf8')
  }
}
/* ============================================================
   🟪 BLOQUE 6 — DETALLE INDIVIDUAL FULL (PERFIL ASESOR)
   ============================================================ */

export async function getPayrollDetailRowFull({ period, user_id }) {
  const per = parsePeriod(period)
  if (!per) throw new Error('Periodo inválido. Usa YYYY-MM')

  const uid = Number(user_id)
  if (!uid) throw new Error('Debe proveer un user_id válido')

  const client = await pool.connect()
  try {
    const sql = `
      WITH month_bounds AS (
        SELECT
          make_date($1, $2, 1)::date                                AS start_month,
          (make_date($1, $2, 1) + INTERVAL '1 month - 1 day')::date AS end_month,
          EXTRACT(
            DAY FROM (make_date($1, $2, 1) + INTERVAL '1 month - 1 day')
          )::int AS days_in_month
      ),

      siapp_sales AS (
        SELECT
          COUNT(*) FILTER (WHERE s.in_district = true)  AS real_in_count,
          COUNT(*) FILTER (WHERE s.in_district = false) AS real_out_count,
          COUNT(*)                                       AS real_total_count
        FROM siapp.full_sales s
        WHERE s.user_id = $3
          AND s.period_year = $1
          AND s.period_month = $2
      ),

      last_nomina AS (
        SELECT DISTINCT ON (a.cedula)
          a.cedula::text,
          a.presupuesto_mes::numeric(10,2) AS s_presupuesto_mes,
          a.dias_laborados::int            AS s_dias_laborados,
          a.novedad                        AS s_novedad,
          a.fecha_inicio_contrato          AS s_contract_start,
          a.fecha_fin_contrato             AS s_contract_end
        FROM staging.archivo_nomina a
        ORDER BY a.cedula, a.imported_at DESC, a.raw_row DESC
      ),

      nov_core AS (
        SELECT
          n.user_id,
          STRING_AGG(
            n.novelty_type || ' ' ||
            to_char(n.start_date,'YYYY-MM-DD') || '→' ||
            to_char(n.end_date,'YYYY-MM-DD'),
            ' | '
          ) AS novedades
        FROM core.user_novelties n
        JOIN month_bounds b ON (n.start_date, n.end_date)
            OVERLAPS (b.start_month, b.end_month)
        GROUP BY n.user_id
      ),

      contract_eval AS (
        SELECT
          u.id AS user_id,
          CASE
            WHEN COALESCE(u.contract_start, ln.s_contract_start) IS NULL
              AND COALESCE(u.contract_end, ln.s_contract_end) IS NULL THEN TRUE

            WHEN COALESCE(u.contract_start, ln.s_contract_start) IS NOT NULL
             AND COALESCE(u.contract_end, ln.s_contract_end) IS NULL
             AND COALESCE(u.contract_start, ln.s_contract_start)
                 <= (SELECT end_month FROM month_bounds) THEN TRUE

            WHEN COALESCE(u.contract_start, ln.s_contract_start) IS NULL
             AND COALESCE(u.contract_end, ln.s_contract_end) IS NOT NULL
             AND COALESCE(u.contract_end, ln.s_contract_end)
                 >= (SELECT start_month FROM month_bounds) THEN TRUE

            WHEN (COALESCE(u.contract_start, ln.s_contract_start),
                  COALESCE(u.contract_end, ln.s_contract_end))
                  OVERLAPS (
                    (SELECT start_month FROM month_bounds),
                    (SELECT end_month FROM month_bounds)
                  ) THEN TRUE

            ELSE FALSE
          END AS contratado
        FROM core.users u
        LEFT JOIN last_nomina ln ON ln.cedula = u.document_id::text
        WHERE u.id = $3
      )

      SELECT
        u.id AS user_id,
        u.name AS nombre_funcionario,
        u.document_id AS cedula,
        u.phone,
        u.email,
        u.district,
        u.district_claro,

        ou.unit_type AS unidad_tipo,
        ou.name AS unidad_nombre,

        /* NOMINA */
        um.presupuesto_mes AS um_presupuesto_mes,
        um.dias_laborados AS um_dias_laborados,
        um.prorrateo AS um_prorrateo,

        /* PROGRESS */
        p.expected_count,
        p.adjusted_count,
        p.met_in_district,
        p.met_global,
        p.compliance_in_percent,
        p.compliance_global_percent,

        /* SIAPP FULL */
        ss.real_in_count,
        ss.real_out_count,
        ss.real_total_count,

        /* NOVEDADES */
        COALESCE(nc.novedades, ln.s_novedad, '') AS novedades,

        /* CONTRATO */
        ce.contratado AS contratado_flag,

        /* FALLBACK de NOMINA STAGING */
        ln.s_presupuesto_mes,
        ln.s_dias_laborados,
        ln.s_contract_start,
        ln.s_contract_end

      FROM core.users u
      LEFT JOIN core.org_units ou ON ou.id = u.org_unit_id
      LEFT JOIN core.user_monthly um
        ON um.user_id = u.id
       AND um.period_year = $1
       AND um.period_month = $2
      LEFT JOIN core.progress p
        ON p.user_id = u.id
       AND p.period_year = $1
       AND p.period_month = $2
      LEFT JOIN siapp_sales ss ON true
      LEFT JOIN last_nomina ln ON ln.cedula = u.document_id::text
      LEFT JOIN nov_core nc ON nc.user_id = u.id
      LEFT JOIN contract_eval ce ON ce.user_id = u.id
      WHERE u.id = $3
      LIMIT 1;
    `

    const { rows } = await client.query(sql, [
      per.year,
      per.month,
      uid
    ])

    const r = rows[0]
    if (!r) return null

    /* ------------------------------------------------------------
       Cálculos finales
    ------------------------------------------------------------ */

    const presupuesto_mes =
      r.um_presupuesto_mes ??
      r.s_presupuesto_mes ??
      13

    const dias31 =
      r.um_dias_laborados ??
      r.s_dias_laborados ??
      30

    const prorrateo =
      r.um_prorrateo ??
      Math.round(
        presupuesto_mes * (dias31 / 30)
      )

    const garantizadoParaComisionar = r.expected_count ?? null
    const garantizadoConNovedades   = r.adjusted_count ?? null

    const ventasDistrito = r.real_in_count ?? 0
    const ventasFuera    = r.real_out_count ?? 0
    const totalVentas    = r.real_total_count ?? 0

    const difDistrito =
      ventasDistrito - (garantizadoParaComisionar ?? 0)

    const difTotal =
      totalVentas - (garantizadoConNovedades ?? 0)

    /* ------------------------------------------------------------
       Respuesta final — JSON profesional FULL
    ------------------------------------------------------------ */

    return {
      user_id: r.user_id,
      nombre_funcionario: r.nombre_funcionario,
      cedula: r.cedula,

      distrito: r.district,
      distrito_claro: r.district_claro,
      unidad_tipo: r.unidad_tipo,
      unidad_nombre: r.unidad_nombre,

      contratado_si_no: r.contratado_flag ? 'SI' : 'NO',
      estado: r.contratado_flag ? 'ACTIVO' : 'RETIRADO',

      fecha_inicio_contrato:
        r.s_contract_start ?? null,

      fecha_fin_contrato:
        r.s_contract_end ?? null,

      novedades: r.novedades,

      presupuesto_mes: Number(presupuesto_mes),
      dias_laborados_31: Number(dias31),
      prorrateo_novedades: Number(prorrateo),

      garantizado_para_comisionar:
        garantizadoParaComisionar != null
          ? Number(garantizadoParaComisionar)
          : null,

      garantizado_con_novedades:
        garantizadoConNovedades != null
          ? Number(garantizadoConNovedades)
          : null,

      ventas_distrito: Number(ventasDistrito),
      ventas_fuera_distrito: Number(ventasFuera),
      total_ventas: Number(totalVentas),

      diferencia_en_distrito: Number(difDistrito),
      diferencia_total: Number(difTotal),

      cumple_distrito_zonificado:
        r.met_in_district ? 'CUMPLE' : 'NO CUMPLE',

      cumple_global:
        r.met_global ? 'CUMPLE' : 'NO CUMPLE',

      compliance_in_percent: r.compliance_in_percent,
      compliance_global_percent: r.compliance_global_percent
    }
  } finally {
    client.release()
  }
}
/* ============================================================
   🟩 BLOQUE 7 — BASIC PARA COORDINADOR (FULL + OPTIMIZADO)
   ============================================================ */

export async function fetchBasicForCoordinatorFull({
  period_year,
  period_month,
  coordinator_id
}) {
  const client = await pool.connect()
  try {
    const sql = `
      WITH month_bounds AS (
        SELECT
          make_date($1,$2,1)::date AS start_month,
          (make_date($1,$2,1) + INTERVAL '1 month - 1 day')::date AS end_month,
          EXTRACT(
            DAY FROM (make_date($1,$2,1) + INTERVAL '1 month - 1 day')
          )::int AS days_in_month
      ),

      siapp_sales AS (
        SELECT
          s.user_id,
          COUNT(*) FILTER (WHERE s.in_district = true)  AS real_in_count,
          COUNT(*) FILTER (WHERE s.in_district = false) AS real_out_count,
          COUNT(*)                                       AS real_total_count
        FROM siapp.full_sales s
        WHERE s.period_year = $1
          AND s.period_month = $2
        GROUP BY s.user_id
      ),

      last_nomina AS (
        SELECT DISTINCT ON (a.cedula)
          a.cedula::text,
          a.presupuesto_mes::numeric(10,2) AS s_presupuesto_mes,
          a.dias_laborados::int            AS s_dias_laborados,
          a.novedad                        AS s_novedad
        FROM staging.archivo_nomina a
        ORDER BY a.cedula, a.imported_at DESC, a.raw_row DESC
      ),

      nov_core AS (
        SELECT
          n.user_id,
          STRING_AGG(
            n.novelty_type || ' ' ||
            to_char(n.start_date,'YYYY-MM-DD') || '→' ||
            to_char(n.end_date,'YYYY-MM-DD'),
            ' | '
          ) AS novedades
        FROM core.user_novelties n
        JOIN month_bounds b ON (n.start_date,n.end_date)
             OVERLAPS (b.start_month,b.end_month)
        GROUP BY n.user_id
      )

      SELECT
        u.id AS user_id,
        u.name AS nombre,
        u.phone,
        u.email,
        u.district_claro,
        ou.unit_type,
        ou.name AS unidad,

        /* payroll */
        COALESCE(um.presupuesto_mes, ln.s_presupuesto_mes, 13) AS presupuesto_mes,
        COALESCE(um.dias_laborados, ln.s_dias_laborados, 30)   AS dias_laborados,
        COALESCE(
          um.prorrateo,
          ROUND(
            COALESCE(um.presupuesto_mes, ln.s_presupuesto_mes, 13)::numeric *
            (
              COALESCE(um.dias_laborados, ln.s_dias_laborados, 30)::numeric /
              (SELECT days_in_month FROM month_bounds)
            )
          )
        ) AS prorrateo_calc,

        /* ventas */
        COALESCE(ss.real_in_count,0) AS real_in_count,
        COALESCE(ss.real_out_count,0) AS real_out_count,
        COALESCE(ss.real_total_count,0) AS real_total_count,

        /* cumplimiento */
        p.compliance_global_percent,
        p.expected_count,
        p.adjusted_count,

        /* novedades */
        COALESCE(nc.novedades, ln.s_novedad, '') AS novedades

      FROM core.users u
      LEFT JOIN core.org_units ou
        ON ou.id = u.org_unit_id

      LEFT JOIN core.user_monthly um
        ON um.user_id = u.id
       AND um.period_year = $1
       AND um.period_month = $2

      LEFT JOIN core.progress p
        ON p.user_id = u.id
       AND p.period_year = $1
       AND p.period_month = $2

      LEFT JOIN siapp_sales ss ON ss.user_id = u.id
      LEFT JOIN last_nomina ln ON ln.cedula = u.document_id::text
      LEFT JOIN nov_core nc ON nc.user_id = u.id

      WHERE u.role = 'ASESORIA'
        AND u.coordinator_id = $3

      ORDER BY u.name;
    `

    const { rows } = await client.query(sql, [
      period_year,
      period_month,
      coordinator_id
    ])

    /* ----------------------------------------------------------
       Ensamblado final — versión FULL ENTERPRISE
       ---------------------------------------------------------- */
    return rows.map(r => ({
      user_id: r.user_id,
      nombre: r.nombre,
      phone: r.phone,
      email: r.email,
      distrito_claro: r.district_claro,

      unidad: {
        tipo: r.unit_type,
        nombre: r.unidad
      },

      presupuesto_mes: Number(r.presupuesto_mes),
      dias_laborados_31: Number(r.dias_laborados),
      prorrateo_mes: Number(r.prorrateo_calc),

      conexiones_mes: {
        in: Number(r.real_in_count),
        out: Number(r.real_out_count),
        total: Number(r.real_total_count)
      },

      garantizado: {
        base: r.expected_count != null ? Number(r.expected_count) : null,
        con_novedades:
          r.adjusted_count != null ? Number(r.adjusted_count) : null
      },

      cumplimiento_global:
        r.compliance_global_percent != null
          ? Number(r.compliance_global_percent)
          : null,

      novedades: r.novedades || '',

      periodo: {
        year: period_year,
        month: period_month
      }
    }))
  } finally {
    client.release()
  }
}
/* ============================================================
   🟦 BLOQUE 8 — HISTORIAL FULL (PERFIL DEL ASESOR)
   ============================================================ */

export async function fetchHistoryForUserFull(user_id, months_back = 6) {
  const client = await pool.connect()
  try {
    const sql = `
      WITH hist AS (
        SELECT
          period_year,
          period_month,
          compliance_global_percent,
          compliance_in_percent,
          met_global,
          met_in_district,
          expected_count,
          adjusted_count,
          real_total_count
        FROM core.progress
        WHERE user_id = $1
        ORDER BY period_year DESC, period_month DESC
        LIMIT $2
      )
      SELECT *
      FROM hist
      ORDER BY period_year ASC, period_month ASC;
    `

    const { rows } = await client.query(sql, [user_id, months_back])

    /* ----------------------------------------------------------
       ENSAMBLADO FINAL — LISTO PARA GRÁFICAS
       ---------------------------------------------------------- */
    return rows.map(r => ({
      period: `${r.period_year}-${String(r.period_month).padStart(2, '0')}`,

      /* KPI */
      compliance_global: 
        r.compliance_global_percent != null
          ? Number(r.compliance_global_percent)
          : null,

      compliance_in:
        r.compliance_in_percent != null
          ? Number(r.compliance_in_percent)
          : null,

      met_global: r.met_global ? true : false,
      met_in_district: r.met_in_district ? true : false,

      /* valores crudos */
      expected: r.expected_count != null ? Number(r.expected_count) : null,
      adjusted: r.adjusted_count != null ? Number(r.adjusted_count) : null,
      real: r.real_total_count != null ? Number(r.real_total_count) : null
    }))
  } finally {
    client.release()
  }
}
export async function generatePayrollReportFULL({
  period,
  unit_id = null,
  format = 'xlsx'
}) {
  const per = parsePeriod(period)
  if (!per) throw new Error('Periodo invalido. Usa YYYY-MM')

  const detailRows = await fetchReportRowsFull({
    period_year: per.year,
    period_month: per.month,
    unit_id
  })

  const kpiRows = await fetchKpiRowsFull({
    year: per.year,
    month: per.month,
    unit_id
  })

  if (String(format).toLowerCase() === 'csv') {
    return buildCsvFull({
      detailRows,
      period,
      period_year: per.year,
      period_month: per.month
    })
  }

  const buffer = await buildXlsxFull({
    detailRows,
    kpiRows,
    period_year: per.year,
    period_month: per.month
  })

  return {
    mime: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    filename: `nomina_${period}${unit_id ? `_unit${unit_id}` : ''}.xlsx`,
    buffer
  }
}
export async function generatePayrollReport(args) {
  return generatePayrollReportFULL(args)
}

export async function getPayrollDetailRowFULL({ period, user_id }) {
  return getPayrollDetailRowFull({ period, user_id })
}

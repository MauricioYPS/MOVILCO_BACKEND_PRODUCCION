// services/reports.consolidated.service.js
import pool from '../config/database.js'
import ExcelJS from 'exceljs'

/* -------------------- Helpers -------------------- */

function parsePeriod(q) {
  if (!q) return null
  const m = String(q).match(/^(\d{4})-(\d{1,2})$/)
  if (!m) return null
  return { year: Number(m[1]), month: Number(m[2]) }
}

function pct(n) { return n == null ? null : Number(n) }
function asNum(n) { return n == null ? null : Number(n) }

function monthNameES(m) {
  const names = [
    'ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO',
    'JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE','NOVIEMBRE','DICIEMBRE'
  ]
  return names[(m - 1) % 12]
}

/* ---------------------------------------------------
   AGREGA INFORMACIÓN CONSOLIDADA POR UNIDADES
---------------------------------------------------- */

async function aggForUnits({ year, month, unitIds }) {
  const client = await pool.connect()
  try {
    const { rows } = await client.query(
      `
      WITH prog AS (
        SELECT p.*
        FROM core.progress p
        WHERE p.period_year = $1 AND p.period_month = $2
      ),
      users_sel AS (
        SELECT u.id, u.name, u.org_unit_id
        FROM core.users u
        WHERE u.org_unit_id = ANY($3::int[])
      ),
      joined AS (
        SELECT
          u.org_unit_id,

          COUNT(*) FILTER (WHERE p.user_id IS NOT NULL)         AS evaluados,
          COUNT(*)                                              AS usuarios_total,

          COALESCE(SUM(p.real_in_count), 0)::numeric(18,2)      AS real_in_sum,
          COALESCE(SUM(p.real_out_count), 0)::numeric(18,2)     AS real_out_sum,
          COALESCE(SUM(p.real_total_count), 0)::numeric(18,2)   AS real_total_sum,
          COALESCE(SUM(p.expected_count), 0)::numeric(18,2)     AS expected_sum,
          COALESCE(SUM(p.adjusted_count), 0)::numeric(18,2)     AS adjusted_sum,

          COUNT(*) FILTER (WHERE p.met_in_district IS TRUE)     AS met_in_count,
          COUNT(*) FILTER (WHERE p.met_global IS TRUE)          AS met_global_count,

          AVG(p.compliance_in_percent)                          AS avg_in_percent,
          AVG(p.compliance_global_percent)                      AS avg_global_percent
        FROM users_sel u
        LEFT JOIN prog p ON p.user_id = u.id
        GROUP BY u.org_unit_id
      )

      SELECT
        ou.id,
        ou.name,
        ou.unit_type,
        joined.usuarios_total,
        joined.evaluados,

        joined.real_in_sum,
        joined.real_out_sum,
        joined.real_total_sum,
        joined.expected_sum,
        joined.adjusted_sum,

        joined.met_in_count,
        joined.met_global_count,
        joined.avg_in_percent,
        joined.avg_global_percent

      FROM core.org_units ou
      LEFT JOIN joined ON joined.org_unit_id = ou.id
      WHERE ou.id = ANY($3::int[])
      ORDER BY ou.unit_type, ou.name
      `,
      [year, month, unitIds]
    )

    return rows.map(r => ({
      unit_id: Number(r.id),
      unidad: r.name,
      tipo: r.unit_type,

      usuarios_total: asNum(r.usuarios_total) ?? 0,
      evaluados: asNum(r.evaluados) ?? 0,
      cobertura_percent:
        (r.usuarios_total && r.evaluados != null)
        ? Number((r.evaluados / r.usuarios_total) * 100)
        : 0,

      real_in_sum: asNum(r.real_in_sum) ?? 0,
      real_out_sum: asNum(r.real_out_sum) ?? 0,
      real_total_sum: asNum(r.real_total_sum) ?? 0,
      expected_sum: asNum(r.expected_sum) ?? 0,
      adjusted_sum: asNum(r.adjusted_sum) ?? 0,

      met_in_count: asNum(r.met_in_count) ?? 0,
      met_global_count: asNum(r.met_global_count) ?? 0,

      avg_in_percent: pct(r.avg_in_percent),
      avg_global_percent: pct(r.avg_global_percent)
    }))
  } finally {
    client.release()
  }
}

/* ---------------------------------------------------
   LISTAR UNIDADES POR NIVEL
---------------------------------------------------- */

async function listUnitsByLevel({ level, parent_id = null }) {
  const client = await pool.connect()
  try {
    if (parent_id) {
      const { rows } = await client.query(
        `
        WITH RECURSIVE sub AS (
          SELECT id FROM core.org_units WHERE id = $1
          UNION ALL
          SELECT ou.id
          FROM core.org_units ou
          JOIN sub s ON ou.parent_id = s.id
        )
        SELECT id, name, unit_type
        FROM core.org_units
        WHERE parent_id = $1 AND unit_type = $2
        ORDER BY name
        `,
        [parent_id, level]
      )
      return rows.map(r => ({
        id: r.id,
        name: r.name,
        unit_type: r.unit_type
      }))
    } else {
      const { rows } = await client.query(
        `
        SELECT id, name, unit_type
        FROM core.org_units
        WHERE unit_type = $1
        ORDER BY name
        `,
        [level]
      )
      return rows.map(r => ({
        id: r.id,
        name: r.name,
        unit_type: r.unit_type
      }))
    }
  } finally {
    client.release()
  }
}

/* ---------------------------------------------------
   GENERAR UNIVERSO (ÁRBOL) SEGÚN SCOPE
---------------------------------------------------- */

async function universeForScope({ scope, unit_id = null }) {
  if (scope === 'company') {
    return {
      ger: await listUnitsByLevel({ level: 'GERENCIA' }),
      dir: await listUnitsByLevel({ level: 'DIRECCION' }),
      cor: await listUnitsByLevel({ level: 'COORDINACION' })
    }
  }

  if (scope === 'gerencia' && unit_id) {
    return {
      ger: [],
      dir: await listUnitsByLevel({ level: 'DIRECCION', parent_id: unit_id }),
      cor: await listUnitsByLevel({ level: 'COORDINACION', parent_id: unit_id })
    }
  }

  if (scope === 'direccion' && unit_id) {
    return {
      ger: [],
      dir: [],
      cor: await listUnitsByLevel({ level: 'COORDINACION', parent_id: unit_id })
    }
  }

  return {
    ger: await listUnitsByLevel({ level: 'GERENCIA' }),
    dir: await listUnitsByLevel({ level: 'DIRECCION' }),
    cor: await listUnitsByLevel({ level: 'COORDINACION' })
  }
}

/* ---------------------------------------------------
   GENERAR CONSOLIDADO
---------------------------------------------------- */

async function fetchConsolidated({ year, month, scope = 'company', unit_id = null }) {
  const tree = await universeForScope({ scope, unit_id })

  const allUnits = [
    ...tree.ger.map(u => u.id),
    ...tree.dir.map(u => u.id),
    ...tree.cor.map(u => u.id)
  ]

  const base = await aggForUnits({ year, month, unitIds: allUnits })
  const byId = new Map(base.map(r => [r.unit_id, r]))

  return {
    gerencias: tree.ger.map(u => byId.get(u.id)).filter(Boolean),
    direcciones: tree.dir.map(u => byId.get(u.id)).filter(Boolean),
    coordinaciones: tree.cor.map(u => byId.get(u.id)).filter(Boolean)
  }
}

/* ---------------------------------------------------
   CSV GENERATOR
---------------------------------------------------- */

function rowsToCsv(headers, rows) {
  const lineH = headers.join(',')
  const lines = [lineH]

  for (const r of rows) {
    const vals = headers.map(h => {
      const v = r[h]
      if (v == null) return ''
      if (typeof v === 'string') return `"${v.replace(/"/g, '""')}"`
      return String(v)
    })
    lines.push(vals.join(','))
  }

  return Buffer.from(lines.join('\n'), 'utf8')
}

/* ---------------------------------------------------
   ORDENADOR GLOBAL
---------------------------------------------------- */

function sortAll(rows) {
  const order = { GERENCIA: 1, DIRECCION: 2, COORDINACION: 3 }
  return rows.sort((a, b) => {
    const ta = order[a.tipo] ?? 99
    const tb = order[b.tipo] ?? 99
    if (ta !== tb) return ta - tb
    return (a.unidad || '').localeCompare(b.unidad || '', 'es')
  })
}

/* ---------------------------------------------------
   GENERAR EXCEL COMPLETO
---------------------------------------------------- */

async function buildXlsx({ data, period_year, period_month, scope, unit_id }) {
  const wb = new ExcelJS.Workbook()

  /* -------------------- HOJA 1: CONSOLIDADO -------------------- */
  const ws1 = wb.addWorksheet('Consolidado', {
    properties: { defaultRowHeight: 18 }
  })

  const headers = [
    'Tipo Unidad','Unidad','Usuarios Totales','Evaluados','Cobertura %',
    'Accesos IN','Accesos OUT','Accesos TOTAL',
    'Meta Esperada Total','Meta Ajustada Total',
    '% Cumpl. Distrito (prom)','% Cumpl. Global (prom)',
    'Cumplen en Distrito','Cumplen Global'
  ]

  ws1.addRow(headers).font = { bold: true }

  const all = sortAll([
    ...(data.gerencias || []).map(r => ({ tipo: 'GERENCIA', ...r })),
    ...(data.direcciones || []).map(r => ({ tipo: 'DIRECCION', ...r })),
    ...(data.coordinaciones || []).map(r => ({ tipo: 'COORDINACION', ...r }))
  ])

  for (const r of all) {
    ws1.addRow([
      r.tipo,
      r.unidad,
      r.usuarios_total,
      r.evaluados,
      r.cobertura_percent != null ? Number(r.cobertura_percent) / 100 : '',

      r.real_in_sum,
      r.real_out_sum,
      r.real_total_sum,

      r.expected_sum,
      r.adjusted_sum,

      r.avg_in_percent != null ? Number(r.avg_in_percent) / 100 : '',
      r.avg_global_percent != null ? Number(r.avg_global_percent) / 100 : '',

      r.met_in_count,
      r.met_global_count
    ])
  }

  const widths = [14,28,16,12,14,14,14,16,18,18,18,18,18,16]
  widths.forEach((w, i) => { ws1.getColumn(i + 1).width = w })

  ws1.autoFilter = { from: 'A1', to: `N1` }

  /* FORMATO CONDICIONAL */
  const startRow = 2
  const endRow = startRow + all.length - 1

  if (all.length > 0) {
    for (let r = startRow; r <= endRow; r++) {
      const cCob = ws1.getCell(r, 5)
      const cAvg = ws1.getCell(r, 12)

      if (typeof cCob.value === 'number') cCob.numFmt = '0.00%'
      if (typeof cAvg.value === 'number') cAvg.numFmt = '0.00%'

      const fills = {
        good: { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFD9F2D9' } },
        warn: { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFF2CC' } },
        bad:  { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF8D7DA' } },
      }

      if (typeof cCob.value === 'number') {
        const v = cCob.value
        cCob.fill = v >= 1 ? fills.good : v >= 0.8 ? fills.warn : fills.bad
      }
      if (typeof cAvg.value === 'number') {
        const v = cAvg.value
        cAvg.fill = v >= 1 ? fills.good : v >= 0.8 ? fills.warn : fills.bad
      }
    }
  }

  /* -------------------- HOJA 2: ALERTAS -------------------- */
  const ws2 = wb.addWorksheet('Alertas')

  ws2.addRow([
    'Tipo','Unidad','Cobertura %','% Cumpl Global',
    'Accesos TOTAL','Meta Ajustada Total'
  ]).font = { bold: true }

  const alerts = (all || [])
    .map(r => ({
      tipo: r.tipo,
      unidad: r.unidad,
      cobertura: r.cobertura_percent != null ? Number(r.cobertura_percent) / 100 : null,
      cumpl: r.avg_global_percent != null ? Number(r.avg_global_percent) / 100 : null,
      real: r.real_total_sum,
      ajustada: r.adjusted_sum
    }))
    .filter(a =>
      (typeof a.cobertura === 'number' && a.cobertura < 1) ||
      (typeof a.cumpl === 'number' && a.cumpl < 1) ||
      (a.ajustada > a.real)
    )

  for (const a of alerts) {
    ws2.addRow([
      a.tipo,
      a.unidad,
      typeof a.cobertura === 'number' ? a.cobertura : '',
      typeof a.cumpl === 'number' ? a.cumpl : '',
      a.real,
      a.ajustada
    ])
  }

  ws2.autoFilter = { from: 'A1', to: 'F1' }

  for (let r = 2; r <= ws2.rowCount; r++) {
    const cCob = ws2.getCell(r, 3)
    const cCmpl = ws2.getCell(r, 4)

    if (typeof cCob.value === 'number') {
      cCob.numFmt = '0.00%'
      if (cCob.value < 1) cCob.fill = { type:'pattern', pattern:'solid', fgColor:{argb:'FFF8D7DA'} }
    }

    if (typeof cCmpl.value === 'number') {
      cCmpl.numFmt = '0.00%'
      if (cCmpl.value < 1) cCmpl.fill = { type:'pattern', pattern:'solid', fgColor:{argb:'FFF8D7DA'} }
    }
  }

  /* -------------------- HOJA 3: RESUMEN MES -------------------- */
  const ws3 = wb.addWorksheet('Resumen Mes')

  ws3.addRow([`Periodo: ${period_year}-${String(period_month).padStart(2, '0')} (${monthNameES(period_month)})`])
  ws3.addRow([`Scope: ${scope}${unit_id ? '  unit_id=' + unit_id : ''}`])

  const buf = await wb.xlsx.writeBuffer()
  return buf
}

/* ---------------------------------------------------
   EXPORTADOR PRINCIPAL
---------------------------------------------------- */

export async function generateConsolidatedReport({ period, scope = 'company', unit_id = null, format = 'xlsx' }) {
  const per = parsePeriod(period)
  if (!per) throw new Error('Periodo inválido. Usa YYYY-MM')

  const data = await fetchConsolidated({
    year: per.year,
    month: per.month,
    scope,
    unit_id
  })

  if (format === 'csv') {
    const headers = [
      'tipo','unidad','usuarios_total','evaluados','cobertura_percent',
      'real_in_sum','real_out_sum','real_total_sum',
      'expected_sum','adjusted_sum',
      'avg_in_percent','avg_global_percent',
      'met_in_count','met_global_count'
    ]

    const rows = [
      ...(data.gerencias || []).map(r => ({ tipo: 'GERENCIA', unidad: r.unidad, ...r })),
      ...(data.direcciones || []).map(r => ({ tipo: 'DIRECCION', unidad: r.unidad, ...r })),
      ...(data.coordinaciones || []).map(r => ({ tipo: 'COORDINACION', unidad: r.unidad, ...r }))
    ]

    return {
      mime: 'text/csv; charset=utf-8',
      filename: `consolidado_${period}${unit_id ? `_unit${unit_id}` : ''}.csv`,
      buffer: rowsToCsv(headers, rows)
    }
  }

  const xlsx = await buildXlsx({
    data,
    period_year: per.year,
    period_month: per.month,
    scope,
    unit_id
  })

  return {
    mime: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    filename: `consolidado_${period}${unit_id ? `_unit${unit_id}` : ''}.xlsx`,
    buffer: Buffer.from(xlsx)
  }
}

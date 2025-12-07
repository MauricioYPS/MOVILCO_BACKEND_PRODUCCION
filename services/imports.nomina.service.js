// services/imports.nomina.service.js
import ExcelJS from 'exceljs'
import pool from '../config/database.js'

const SHEET_NAME = 'Archivo Nomina' // exacto como aparece en la pestaña

// --- helpers de normalización ---
function normalize(s) {
  if (s == null) return ''
  return String(s)
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // sin acentos
    .replace(/\s+/g, ' ')
    .trim()
    .toUpperCase()
}

function toDate(v) {
  if (v == null || v === '') return null
  if (v instanceof Date) return v.toISOString().slice(0, 10)
  // ExcelJS a veces trae objetos con .text o números (serial excel)
  if (typeof v === 'number') {
    // serial Excel (1900 system)
    const epoch = new Date(Date.UTC(1899, 11, 30))
    const date = new Date(epoch.getTime() + v * 24 * 60 * 60 * 1000)
    return date.toISOString().slice(0, 10)
  }
  const t = normalize(v)
  // formatos comunes DD/MM/YYYY o YYYY-MM-DD
  const m1 = t.match(/^(\d{2})[\/\-](\d{2})[\/\-](\d{4})$/)
  if (m1) {
    const [ , dd, mm, yyyy ] = m1
    return `${yyyy}-${mm}-${dd}`
  }
  const m2 = t.match(/^(\d{4})[\/\-](\d{2})[\/\-](\d{2})$/)
  if (m2) return `${m2[1]}-${m2[2]}-${m2[3]}`
  return null
}

function toNumber(v) {
  if (v == null || v === '') return null
  if (typeof v === 'number') return v
  const t = String(v).replace(/[^0-9\.\-]/g, '')
  const n = Number(t)
  return Number.isFinite(n) ? n : null
}

// --- mapeo flexible por encabezados ---
const wanted = {
  cedula:              ['CEDULA','CC','DOCUMENTO'],
  nombre:              ['NOMBRE DE FUNCIONARIO','NOMBRE FUNCIONARIO','FUNCIONARIO'],
  contratado:          ['CONTRATADO','ESTADO','ACTIVO','RETIRADO'],
  distrito:            ['DISTRITO'],
  distrito_claro:      ['DISTRITO CLARO','DISTRITO DECLARO','DISTRITO DECLARO*'],
  fecha_inicio:        ['FECHA INICIO CONTRATO','INICIO CONTRATO'],
  fecha_fin:           ['FECHA FIN CONTRATO','FIN CONTRATO'],
  novedad:             ['NOVEDAD','NOVEDADES'],
  presupuesto_mes:     ['PRESUPUESTO MES'],
  dias_laborados:      ['DIAS LABORADOS','DIAS LABORADOS AL 31','DIAS LABORADOS AL 31 MES'],
  prorrateo:           ['PRORRATEO','PRORRATEO SEGUN NOVEDADES'],
  estado_envio:        ['ESTADO ENVIO PRESUPUESTO','ESTADO ENVIO']
}

function findHeaderRow(ws) {
  // escanea primeras 30 filas buscando “CEDULA” y “NOMBRE …”
  for (let r = 1; r <= Math.min(30, ws.rowCount); r++) {
    const row = ws.getRow(r)
    const texts = row.values.map(v => normalize(v))
    const hasCedula = texts.some(t => t.includes('CEDULA'))
    const hasNombre = texts.some(t => t.includes('NOMBRE'))
    if (hasCedula && hasNombre) return r
  }
  return null
}

function buildColumnIndex(ws, headerRow) {
  const row = ws.getRow(headerRow)
  const map = {}
  const cols = row.values.map(v => normalize(v))
  const findAny = (keywords) => {
    for (let c = 1; c < cols.length; c++) {
      const cellText = cols[c]
      for (const k of keywords) {
        if (cellText.includes(normalize(k))) return c
      }
    }
    return -1
  }
  map.cedula          = findAny(wanted.cedula)
  map.nombre          = findAny(wanted.nombre)
  map.contratado      = findAny(wanted.contratado)
  map.distrito        = findAny(wanted.distrito)
  map.distrito_claro  = findAny(wanted.distrito_claro)
  map.fecha_inicio    = findAny(wanted.fecha_inicio)
  map.fecha_fin       = findAny(wanted.fecha_fin)
  map.novedad         = findAny(wanted.novedad)
  map.presupuesto     = findAny(wanted.presupuesto_mes)
  map.dias_lab        = findAny(wanted.dias_laborados)
  map.prorrateo       = findAny(wanted.prorrateo)
  map.estado_envio    = findAny(wanted.estado_envio)
  return map
}

function rowHasAnyValue(r) {
  return Object.values(r).some(v => v != null && v !== '')
}

export async function importNominaFromExcel(buffer) {
  // 1) leer libro
  const wb = new ExcelJS.Workbook()
  await wb.xlsx.load(buffer)

  const ws = wb.getWorksheet(SHEET_NAME)
  if (!ws) throw new Error(`No se encontró la hoja "${SHEET_NAME}" en el Excel`)

  // 2) detectar fila de encabezado
  const headerRow = findHeaderRow(ws)
  if (!headerRow) throw new Error('No se pudo detectar la fila de encabezados (no se encontró CEDULA/NOMBRE)')

  // 3) construir índice de columnas
  const idx = buildColumnIndex(ws, headerRow)

  // sanity: al menos cédula y nombre
  if (idx.cedula < 0 || idx.nombre < 0) {
    throw new Error('Encabezados insuficientes: no se ubicaron columnas de CEDULA y/o NOMBRE')
  }

  // 4) recorrer filas de datos
  const rows = []
  for (let r = headerRow + 1; r <= ws.rowCount; r++) {
    const row = ws.getRow(r)
    // saltar filas totalmente en blanco
    const rawCheck = (row.values || []).slice(1).some(v => v != null && String(v).trim() !== '')
    if (!rawCheck) continue

    const rec = {
      raw_row: r - headerRow,
      cedula: idx.cedula > 0 ? normalize(row.getCell(idx.cedula).value).replace(/\D/g,'') || null : null,
      nombre_funcionario: idx.nombre > 0 ? String(row.getCell(idx.nombre).value || '').toString().trim() || null : null,
      contratado: idx.contratado > 0 ? normalize(row.getCell(idx.contratado).value) || null : null,
      distrito: idx.distrito > 0 ? String(row.getCell(idx.distrito).value || '').toString().trim() || null : null,
      distrito_claro: idx.distrito_claro > 0 ? String(row.getCell(idx.distrito_claro).value || '').toString().trim() || null : null,
      fecha_inicio_contrato: idx.fecha_inicio > 0 ? toDate(row.getCell(idx.fecha_inicio).value) : null,
      fecha_fin_contrato: idx.fecha_fin > 0 ? toDate(row.getCell(idx.fecha_fin).value) : null,
      novedad: idx.novedad > 0 ? String(row.getCell(idx.novedad).value || '').toString().trim() || null : null,
      presupuesto_mes: idx.presupuesto > 0 ? toNumber(row.getCell(idx.presupuesto).value) : null,
      dias_laborados: idx.dias_lab > 0 ? toNumber(row.getCell(idx.dias_lab).value) : null,
      prorrateo: idx.prorrateo > 0 ? toNumber(row.getCell(idx.prorrateo).value) : null,
      estado_envio_presupuesto: idx.estado_envio > 0 ? String(row.getCell(idx.estado_envio).value || '').toString().trim() || null : null,
    }
    if (rowHasAnyValue(rec)) rows.push(rec)
  }

  // 5) insertar en staging
  const client = await pool.connect()
  try {
    await client.query('BEGIN')
    await client.query('TRUNCATE TABLE staging.archivo_nomina')

    const text = `
      INSERT INTO staging.archivo_nomina
      (raw_row, cedula, nombre_funcionario, contratado, distrito, distrito_claro,
       fecha_inicio_contrato, fecha_fin_contrato, novedad, presupuesto_mes,
       dias_laborados, prorrateo, estado_envio_presupuesto)
      VALUES
      ${rows.map((_,i)=>`($${i*13+1},$${i*13+2},$${i*13+3},$${i*13+4},$${i*13+5},$${i*13+6},$${i*13+7},$${i*13+8},$${i*13+9},$${i*13+10},$${i*13+11},$${i*13+12},$${i*13+13})`).join(',')}
    `
    const values = rows.flatMap(r => [
      r.raw_row,
      r.cedula,
      r.nombre_funcionario,
      r.contratado,
      r.distrito,
      r.distrito_claro,
      r.fecha_inicio_contrato,
      r.fecha_fin_contrato,
      r.novedad,
      r.presupuesto_mes,
      r.dias_laborados,
      r.prorrateo,
      r.estado_envio_presupuesto
    ])
    if (rows.length > 0) await client.query(text, values)
    await client.query('COMMIT')
    return { inserted: rows.length, headerRow, columns: idx }
  } catch (e) {
    await client.query('ROLLBACK')
    throw e
  } finally {
    client.release()
  }
}

// services/imports.service.js
import ExcelJS from 'exceljs'
import pool from '../config/database.js'

// ----------------- Mapeo dataset -> tabla staging permitida -----------------
const DATASET_TABLE = {
  estructura: 'staging.estructura_jerarquia',
  presupuesto: 'staging.presupuesto_jerarquia',
  novedades: 'staging.novedades',
  nomina: 'staging.archivo_nomina',

  // NUEVO 🚀
  presupuesto_usuarios: 'staging.presupuesto_usuarios'
}

function assertKnownTable(table) {
  if (!table) throw new Error(`Tabla staging no permitida: ${table}`)
  const ok = Object.values(DATASET_TABLE)
  if (!ok.includes(table)) throw new Error(`Tabla staging no permitida: ${table}`)
}

// ----------------- Utils -----------------
function normalize(s) {
  if (s == null) return ''
  return String(s)
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toUpperCase()
}

function cellToText(v) {
  if (v == null) return ''
  if (typeof v === 'object') {
    if (Array.isArray(v.richText)) return v.richText.map(t => t.text || '').join('')
    if (typeof v.text === 'string') return v.text
    if (v.result != null) return String(v.result)
  }
  return String(v)
}

function toDate(v) {
  if (v == null || v === '') return null

  // 1) Si viene como objeto ExcelJS (formula/result/richText), convertir a texto
  if (typeof v === 'object' && !(v instanceof Date)) {
    const asText = cellToText(v)
    if (asText != null && asText !== '') v = asText
  }

  // 2) Date nativo
  if (v instanceof Date) return v.toISOString().slice(0, 10)

  // 3) Serial de Excel (número)
  if (typeof v === 'number') {
    const epoch = new Date(Date.UTC(1899, 11, 30))
    const d = new Date(epoch.getTime() + v * 86400000)
    return d.toISOString().slice(0, 10)
  }

  // 4) String (dd/mm/yyyy o yyyy-mm-dd)
  const t = normalize(v)

  let m = t.match(/^(\d{2})[\/\-](\d{2})[\/\-](\d{4})$/)
  if (m) return `${m[3]}-${m[2]}-${m[1]}`

  m = t.match(/^(\d{4})[\/\-](\d{2})[\/\-](\d{2})$/)
  if (m) return `${m[1]}-${m[2]}-${m[3]}`

  return null
}

function toNumber(v) {
  if (v == null || v === '') return null
  if (typeof v === 'number') return v
  const t = String(v).replace(/[^0-9\.\-]/g, '')
  const n = Number(t)
  return Number.isFinite(n) ? n : null
}

function rowIsEmpty(row) {
  if (!row) return true
  return row.every(v => v == null || String(v).trim() === '')
}

function collectHeaderTexts(ws, headerRow, span = 3) {
  const maxCol = ws.columnCount || (ws.getRow(headerRow)?.values?.length ?? 0)
  const out = Array(maxCol + 1).fill('')
  for (let c = 1; c <= maxCol; c++) {
    const parts = []
    for (let r = headerRow; r < headerRow + span && r <= ws.rowCount; r++) {
      const v = ws.getRow(r)?.getCell(c)?.value
      const t = normalize(cellToText(v))
      if (t) parts.push(t)
    }
    out[c] = parts.join(' ').replace(/\s+/g, ' ').trim()
  }
  return out
}

// ----------------- NOMINA (mínimo para usuarios) -----------------
const NOMINA_SHEET = 'Archivo Nomina'

const NOMINA_WANTED = {
  cedula: ['CEDULA', 'CC', 'DOCUMENTO'],
  nombre: ['NOMBRE DE FUNCIONARIO', 'NOMBRE FUNCIONARIO', 'FUNCIONARIO', 'NOMBRE'],
  distrito: ['DISTRITO'],
  distrito_claro: ['DISTRITO CLARO', 'DISTRITO DECLARO', 'DISTRITO DECLARADO'],
  fecha_inicio: ['FECHA INICIO CONTRATO', 'INICIO CONTRATO', 'FECHA INICIO'],
  telefono: ['TELEFONO', 'TEL', 'CELULAR', 'MOVIL'],
  correo: ['CORREO', 'EMAIL', 'E-MAIL'],
}

function findHeaderRow(ws) {
  const maxScan = Math.min(30, ws.rowCount || 0)
  for (let r = 1; r <= maxScan; r++) {
    const texts = (ws.getRow(r).values || []).map(v => normalize(v ?? ''))
    const hasCedula = texts.some(t => t.includes('CEDULA'))
    const hasNombre = texts.some(t => t.includes('NOMBRE'))
    if (hasCedula && hasNombre) return r
  }
  return null
}

function buildIndex(ws, headerRow, wantedMap) {
  const cols = collectHeaderTexts(ws, headerRow, 3)
  const findAny = (keywords) => {
    for (let c = 1; c < cols.length; c++) {
      const head = cols[c] || ''
      for (const k of keywords) {
        if (head.includes(normalize(k))) return c
      }
    }
    return -1
  }

  return {
    cedula: findAny(wantedMap.cedula),
    nombre: findAny(wantedMap.nombre),
    distrito: findAny(wantedMap.distrito),
    distrito_claro: findAny(wantedMap.distrito_claro),
    fecha_inicio: findAny(wantedMap.fecha_inicio),
    telefono: findAny(wantedMap.telefono),
    correo: findAny(wantedMap.correo),
  }
}

async function parseNomina(filePath) {
  const wb = new ExcelJS.Workbook()
  await wb.xlsx.readFile(filePath)

  const ws = wb.getWorksheet(NOMINA_SHEET)
  if (!ws) throw new Error(`No se encontró la pestaña "${NOMINA_SHEET}"`)

  const headerRow = findHeaderRow(ws)
  if (!headerRow) throw new Error('No se detectó la fila de encabezados')

  const idx = buildIndex(ws, headerRow, NOMINA_WANTED)
  if (idx.cedula < 0 || idx.nombre < 0) {
    throw new Error('Encabezados insuficientes en NOMINA (se requiere CEDULA y NOMBRE)')
  }

  // Import mínimo (solo usuarios)
  const columns = [
    'cedula',
    'nombre_funcionario',
    'contratado',
    'distrito',
    'distrito_claro',
    'fecha_inicio_contrato',
    'telefono',
    'correo'
  ]
  const table = DATASET_TABLE.nomina

  // Helpers: texto seguro para celdas ExcelJS (string/Date/number/object)
  const getCellValue = (row, colIdx) => (colIdx > 0 ? row.getCell(colIdx).value : null)

  const getText = (row, colIdx) => {
    const v = getCellValue(row, colIdx)
    const t = String(cellToText(v) ?? '').trim()
    return t === '' ? null : t
  }

  const getCedula = (row, colIdx) => {
    const v = getCellValue(row, colIdx)
    const t = normalize(cellToText(v) ?? '').replace(/\D/g, '')
    return t === '' ? null : t
  }

  const rows = []
  for (let r = headerRow + 1; r <= ws.rowCount; r++) {
    const row = ws.getRow(r)
    const rawArr = (row.values || []).slice(1)
    if (rowIsEmpty(rawArr)) continue

    const cedula = getCedula(row, idx.cedula)
    const nombre = getText(row, idx.nombre)

    // Reglas mínimas: sin cédula o sin nombre, no importamos
    if (!cedula || !nombre) continue

    let distrito = idx.distrito > 0 ? getText(row, idx.distrito) : null
    let distrito_claro = idx.distrito_claro > 0 ? getText(row, idx.distrito_claro) : null

    // distrito/distrito_claro equivalentes: si viene uno, llena el otro
    if (!distrito && distrito_claro) distrito = distrito_claro
    if (!distrito_claro && distrito) distrito_claro = distrito

    const fecha_inicio = idx.fecha_inicio > 0 ? toDate(getCellValue(row, idx.fecha_inicio)) : null
    const telefono = idx.telefono > 0 ? getText(row, idx.telefono) : null
    const correo = idx.correo > 0 ? getText(row, idx.correo) : null

    rows.push([
      cedula,
      nombre,
      'SI', // contratado fijo (no viene del Excel)
      distrito,
      distrito_claro,
      fecha_inicio,
      telefono,
      correo
    ])
  }

  if (!rows.length) {
    throw new Error('El archivo no contiene registros válidos (revisar CEDULA y NOMBRE).')
  }

  return { table, columns, rows }
}

// ----------------- Parsers simples -----------------
async function parseSimpleFirstRow(filePath, sheetName, table, colMap) {
  const wb = new ExcelJS.Workbook()
  await wb.xlsx.readFile(filePath)
  const ws = wb.getWorksheet(sheetName) || wb.worksheets[0]
  if (!ws) throw new Error(`No se encontró hoja para ${sheetName}`)

  const header = (ws.getRow(1).values || []).slice(1).map(v => normalize(v ?? ''))
  const columns = colMap.map(c => c.to)

  const rows = []
  for (let r = 2; r <= ws.rowCount; r++) {
    const row = ws.getRow(r)
    const raw = (row.values || []).slice(1)
    if (rowIsEmpty(raw)) continue
    const out = []
    for (const m of colMap) {
      const idx = header.findIndex(h => h.includes(normalize(m.from)))
      const val = idx >= 0 ? row.getCell(idx + 1).value : null
      out.push(m.cast ? m.cast(val) : (val ?? null))
    }
    rows.push(out)
  }
  return { table, columns, rows }
}

// ----------------- API pública: extractRows -----------------
export async function extractRows(filePath, dataset) {
  const key = (dataset || '').toLowerCase()
  switch (key) {
    case 'nomina':
      return await parseNomina(filePath)

    case 'estructura':
      return await parseSimpleFirstRow(
        filePath,
        'ESTRUCTURA JERARQUIA',
        DATASET_TABLE.estructura,
        [
          { from: 'NIVEL', to: 'nivel' },
          { from: 'NOMBRE', to: 'nombre' },
          { from: 'PARENT', to: 'parent' },
        ]
      )

    case 'presupuesto':
      return await parseSimpleFirstRow(
        filePath,
        'Presupuesto Jerarquia',
        DATASET_TABLE.presupuesto,
        [
          { from: 'CEDULA', to: 'cedula' },
          { from: 'JERARQUIA', to: 'nivel' },
          { from: 'NOMBRE', to: 'nombre' },
          { from: 'CARGO', to: 'cargo' },
          { from: 'DISTRITO', to: 'distrito' },
          { from: 'REGIONAL', to: 'regional' },
          { from: 'PRESUPUESTO', to: 'presupuesto', cast: toNumber },
          { from: 'TELEFONO', to: 'telefono' },
          { from: 'CORREO', to: 'correo' },
          { from: 'CAPACIDAD', to: 'capacidad', cast: toNumber },
        ]
      )

    case 'novedades':
      return await parseSimpleFirstRow(
        filePath,
        'Archivo Nomina',
        DATASET_TABLE.novedades,
        [
          { from: 'CEDULA', to: 'cedula' },
          { from: 'NOVEDAD', to: 'novedad' },
          { from: 'FECHA INICIO', to: 'fecha_inicio', cast: toDate },
          { from: 'FECHA FIN', to: 'fecha_fin', cast: toDate },
        ]
      )

    // 💥 NUEVO: PRESUPUESTO_USUARIOS
    case 'presupuesto_usuarios':
      return await parseSimpleFirstRow(
        filePath,
        'Presupuesto Jerarquia',
        DATASET_TABLE.presupuesto_usuarios,
        [
          { from: 'JERARQUIA', to: 'jerarquia' },
          { from: 'CARGO', to: 'cargo' },
          { from: 'CEDULA', to: 'cedula' },
          { from: 'NOMBRE', to: 'nombre' },
          { from: 'DISTRITO', to: 'distrito' },
          { from: 'REGIONAL', to: 'regional' },
          { from: 'FECHA INICIO', to: 'fecha_inicio', cast: toDate },
          { from: 'FECHA FIN', to: 'fecha_fin', cast: toDate },
          { from: 'NOVEDADES', to: 'novedades' },
          { from: 'PRESUPUESTO', to: 'presupuesto', cast: toNumber },
          { from: 'CAPACIDAD', to: 'capacidad', cast: toNumber },
          { from: 'TELEFONO', to: 'telefono' },
          { from: 'CORREO', to: 'correo' },
        ]
      )

    default:
      throw new Error(`Dataset no soportado: ${dataset}`)
  }
}

// ----------------- Truncate staging -----------------
export async function truncateStagingTable(table) {
  assertKnownTable(table)
  const client = await pool.connect()
  try {
    await client.query(`TRUNCATE TABLE ${table}`)
  } finally {
    client.release()
  }
}

// ----------------- Bulk Insert -----------------
export async function bulkInsertToStaging(table, columns, rows) {
  assertKnownTable(table)
  if (!rows.length) return 0

  const client = await pool.connect()
  try {
    await client.query('BEGIN')

    const cols = columns.map(c => `"${c}"`).join(',')
    const MAX_PARAMS = 60000
    const chunkSize = Math.max(1, Math.floor(MAX_PARAMS / columns.length))

    for (let offset = 0; offset < rows.length; offset += chunkSize) {
      const chunk = rows.slice(offset, offset + chunkSize)
      const placeholders = chunk
        .map((_, rIdx) => {
          const base = rIdx * columns.length
          const slots = columns.map((__, cIdx) => `$${base + cIdx + 1}`)
          return `(${slots.join(',')})`
        })
        .join(',')

      const values = []
      for (const row of chunk) {
        for (const value of row) values.push(value ?? null)
      }

      const sql = `INSERT INTO ${table} (${cols}) VALUES ${placeholders}`
      await client.query(sql, values)
    }

    await client.query('COMMIT')
    return rows.length
  } catch (e) {
    await client.query('ROLLBACK')
    throw e
  } finally {
    client.release()
  }
}

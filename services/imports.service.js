// services/imports.service.js
import ExcelJS from 'exceljs'
import pool from '../config/database.js'

// ----------------- Mapeo dataset -> tabla staging permitida -----------------
const DATASET_TABLE = {
  estructura: 'staging.estructura_jerarquia',
  presupuesto: 'staging.presupuesto_jerarquia',
  siapp: 'staging.siapp',
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

function toDate(v) {
  if (v == null || v === '') return null
  if (v instanceof Date) return v.toISOString().slice(0, 10)
  if (typeof v === 'number') {
    const epoch = new Date(Date.UTC(1899, 11, 30))
    const d = new Date(epoch.getTime() + v * 86400000)
    return d.toISOString().slice(0, 10)
  }
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

// --- Helpers para encabezados complejos ---
function cellToText(v) {
  if (v == null) return ''
  if (typeof v === 'object') {
    if (Array.isArray(v.richText)) return v.richText.map(t => t.text || '').join('')
    if (typeof v.text === 'string') return v.text
    if (v.result != null) return String(v.result)
  }
  return String(v)
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

// ----------------- NOMINA robusto -----------------
const NOMINA_SHEET = 'Archivo Nomina'

const NOMINA_WANTED = {
  cedula: ['CEDULA', 'CC', 'DOCUMENTO'],
  nombre: ['NOMBRE DE FUNCIONARIO', 'NOMBRE FUNCIONARIO', 'FUNCIONARIO'],
  contratado: ['CONTRATADO', 'ESTADO', 'ACTIVO', 'RETIRADO'],
  distrito: ['DISTRITO'],
  distrito_claro: ['DISTRITO CLARO', 'DISTRITO DECLARO', 'DISTRITO DECLARADO'],
  fecha_inicio: ['FECHA INICIO CONTRATO', 'INICIO CONTRATO'],
  fecha_fin: ['FECHA FIN CONTRATO', 'FIN CONTRATO'],
  novedad: ['NOVEDAD', 'NOVEDADES'],
  presupuesto_mes: ['PRESUPUESTO MES'],
  dias_laborados: ['DIAS LABORADOS', 'DIAS LABORADOS AL 31', 'DIAS LABORADOS AL 31 MES'],
  estado_envio: ['ESTADO ENVIO PRESUPUESTO', 'ESTADO ENVIO']
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
    contratado: findAny(wantedMap.contratado),
    distrito: findAny(wantedMap.distrito),
    distrito_claro: findAny(wantedMap.distrito_claro),
    fecha_inicio: findAny(wantedMap.fecha_inicio),
    fecha_fin: findAny(wantedMap.fecha_fin),
    novedad: findAny(wantedMap.novedad),
    presupuesto_mes: findAny(wantedMap.presupuesto_mes),
    dias_laborados: findAny(wantedMap.dias_laborados),
    estado_envio: findAny(wantedMap.estado_envio),
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
    throw new Error('Encabezados insuficientes en NOMINA')
  }

  const columns = [
    'cedula',
    'nombre_funcionario',
    'contratado',
    'distrito',
    'distrito_claro',
    'fecha_inicio_contrato',
    'fecha_fin_contrato',
    'novedad',
    'presupuesto_mes',
    'dias_laborados',
    'estado_envio_presupuesto'
  ]
  const table = DATASET_TABLE.nomina

  const rows = []
  for (let r = headerRow + 1; r <= ws.rowCount; r++) {
    const row = ws.getRow(r)
    const rawArr = (row.values || []).slice(1)
    if (rowIsEmpty(rawArr)) continue

    const cedRaw = idx.cedula > 0 ? row.getCell(idx.cedula).value : null
    const cedula = normalize(cedRaw).replace(/\D/g, '') || null

    rows.push([
      cedula,
      idx.nombre > 0 ? String(row.getCell(idx.nombre).value ?? '').trim() : null,
      idx.contratado > 0 ? normalize(row.getCell(idx.contratado).value) : null,
      idx.distrito > 0 ? String(row.getCell(idx.distrito).value ?? '').trim() : null,
      idx.distrito_claro > 0 ? String(row.getCell(idx.distrito_claro).value ?? '').trim() : null,
      idx.fecha_inicio > 0 ? toDate(row.getCell(idx.fecha_inicio).value) : null,
      idx.fecha_fin > 0 ? toDate(row.getCell(idx.fecha_fin).value) : null,
      idx.novedad > 0 ? String(row.getCell(idx.novedad).value ?? '').trim() : null,
      idx.presupuesto_mes > 0 ? toNumber(row.getCell(idx.presupuesto_mes).value) : null,
      idx.dias_laborados > 0 ? toNumber(row.getCell(idx.dias_laborados).value) : null,
      idx.estado_envio > 0 ? String(row.getCell(idx.estado_envio).value ?? '').trim() : null,
    ])
  }

  return { table: DATASET_TABLE.nomina, columns, rows }
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

// ----------------- SIAPP robusto -----------------
// (NO SE MODIFICÓ, para no romper nada)

async function parseSiapp(filePath) {
  const wb = new ExcelJS.Workbook()
  await wb.xlsx.readFile(filePath)

  const ws = wb.getWorksheet('Siapp') || wb.getWorksheet('SIAPP') || wb.worksheets[0]
  if (!ws) throw new Error('No se encontró hoja para SIAPP')

  const HEADERS = {
    estado_liquidacion: ['ESTADO LIQUIDACION', 'Estado_Liquidacion'],
    linea_negocio: ['LINEA NEGOCIO', 'Linea_Negocio'],
    cuenta: ['CUENTA'],
    ot: ['OT'],
    id_asesor: ['IDASESOR', 'ID ASESOR'],
    nombre_asesor: ['NOMBRE ASESOR'],
    cant_serv: ['CANT'],
    tipo_red: ['TIPO RED'],
    division: ['DIVISION', 'REGION'],
    area: ['AREA'],
    zona: ['ZONA'],
    poblacion: ['POBLACION'],
    d_distrito: ['DISTRITO'],
    renta: ['RENTA'],
    fecha: ['FECHA'],
    venta: ['VENTA'],
    tipo_registro: ['TIPO REGISTRO'],
    estrato: ['ESTRATO'],
    paquete_pvd: ['PAQUETE PVD'],
    mintic: ['MINTIC'],
    tipo_producto: ['TIPO PRODUCTO'],
    venta_convergente: ['VENTA CONVERGENTE'],
    venta_instale_dth: ['VENTA INSTALE DTH'],
    sac_final: ['SAC FINAL'],
    cedula_vendedor: ['CEDULA VENDEDOR'],
    nombre_vendedor: ['NOMBRE VENDEDOR'],
    modalidad_venta: ['MODALIDAD VENTA'],
    tipo_vendedor: ['TIPO VENDEDOR'],
    tipo_red_comercial: ['TIPO RED COMERCIAL'],
    nombre_regional: ['NOMBRE REGIONAL'],
    nombre_comercial: ['NOMBRE COMERCIAL'],
    nombre_lider: ['NOMBRE LIDER'],
    retencion_control: ['RETENCION CONTROL'],
    observ_retencion: ['OBSERV RETENCION'],
    tipo_contrato: ['TIPO CONTRATO'],
    tarifa_venta: ['TARIFA VENTA'],
    comision_neta: ['COMISION NETA'],
    punto_equilibrio: ['PUNTO EQUILIBRIO']
  }

  // Buscar encabezados
  let headerRow = null
  for (let r = 1; r <= Math.min(20, ws.rowCount); r++) {
    const texts = (ws.getRow(r).values || []).map(v => normalize(v ?? ''))
    if (texts.some(t => t.includes('LINEA')) && texts.some(t => t.includes('CUENTA'))) {
      headerRow = r
      break
    }
  }
  if (!headerRow) throw new Error('No se detectó fila de encabezados en SIAPP')

  const headers = collectHeaderTexts(ws, headerRow, 2)
  const findCol = (keys) => {
    for (let c = 1; c < headers.length; c++) {
      const head = headers[c] || ''
      for (const k of keys) {
        if (head.includes(normalize(k))) return c
      }
    }
    return -1
  }

  const idx = {}
  for (const [key, aliases] of Object.entries(HEADERS)) idx[key] = findCol(aliases)

  const columns = Object.keys(HEADERS)
  const rows = []

  for (let r = headerRow + 1; r <= ws.rowCount; r++) {
    const row = ws.getRow(r)
    const raw = (row.values || []).slice(1)
    if (rowIsEmpty(raw)) continue

    const getText = (i) => (i > 0 ? String(cellToText(row.getCell(i).value) ?? '').trim() : null)
    const getNum = (i) => (i > 0 ? toNumber(row.getCell(i).value) : null)
    const getDate = (i) => (i > 0 ? toDate(row.getCell(i).value) : null)

    rows.push([
      getText(idx.estado_liquidacion),
      getText(idx.linea_negocio),
      getText(idx.cuenta),
      getText(idx.ot),
      getText(idx.id_asesor),
      getText(idx.nombre_asesor),
      getNum(idx.cant_serv),
      getText(idx.tipo_red),
      getText(idx.division),
      getText(idx.area),
      getText(idx.zona),
      getText(idx.poblacion),
      getText(idx.d_distrito),
      getNum(idx.renta),
      getDate(idx.fecha),
      getText(idx.venta),
      getText(idx.tipo_registro),
      getText(idx.estrato),
      getText(idx.paquete_pvd),
      getText(idx.mintic),
      getText(idx.tipo_producto),
      getText(idx.venta_convergente),
      getText(idx.venta_instale_dth),
      getText(idx.sac_final),
      getText(idx.cedula_vendedor),
      getText(idx.nombre_vendedor),
      getText(idx.modalidad_venta),
      getText(idx.tipo_vendedor),
      getText(idx.tipo_red_comercial),
      getText(idx.nombre_regional),
      getText(idx.nombre_comercial),
      getText(idx.nombre_lider),
      getText(idx.retencion_control),
      getText(idx.observ_retencion),
      getText(idx.tipo_contrato),
      getNum(idx.tarifa_venta),
      getNum(idx.comision_neta),
      getNum(idx.punto_equilibrio)
    ])
  }

  return { table: DATASET_TABLE.siapp, columns, rows }
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

    case 'siapp':
      return await parseSiapp(filePath)

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

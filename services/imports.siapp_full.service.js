// services/imports.siapp_full.service.js
import pool from '../config/database.js'
import ExcelJS from 'exceljs'

/** Normaliza nombres de columnas */
function normalize(str) {
  return String(str || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '')
    .replace(/[^a-z0-9_]/g, '')
}

/** Convierte valores a numeric seguro */
function toNumericSafe(v) {
  if (v === null || v === undefined || v === '') return null
  if (typeof v === "number") return v
  const n = Number(String(v).replace(/[^0-9.-]/g, ''))
  return isNaN(n) ? null : n
}

/** Convierte fechas Excel → Date SQL */
function toDateSafe(v) {
  if (!v) return null

  // ExcelJS detecta fechas como objetos Date
  if (v instanceof Date) return v

  // Formato dd/mm/yyyy
  if (typeof v === "string" && v.includes("/")) {
    const [d, m, y] = v.split("/")
    if (d && m && y) {
      const date = new Date(`${y}-${m}-${d}`)
      return isNaN(date.getTime()) ? null : date
    }
  }

  // Excel serial number
  if (typeof v === "number") {
    const excelEpoch = new Date(1899, 11, 30)
    const date = new Date(excelEpoch.getTime() + v * 86400000)
    return date
  }

  return null
}

export async function importSiappFull({ file, source_file }) {
  if (!file) throw new Error("No se recibió archivo")

  const workbook = new ExcelJS.Workbook()
  await workbook.xlsx.load(file.buffer)

  console.log("Sheets:", workbook.worksheets.map(s => `"${s.name}"`));
  console.log("Uploaded file bytes:", file.buffer.length);

  // Buscar hoja SIAPP
  let sheet = workbook.worksheets.find(
    s => String(s.name).trim().toLowerCase() === "siapp"
  )
  if (!sheet) throw new Error("El archivo no contiene una hoja llamada 'Siapp'")

  // Leer encabezados
  const headerRow = sheet.getRow(1)
  const headers = headerRow.values.slice(1)



  const normalizedHeaders = headers.map(h => normalize(h))


  /****************************************************
   * Mapeo de columnas → columnas reales SQL
   ****************************************************/
const allowed = {
  estado_liquidacion: "estado_liquidacion",
  linea_negocio: "linea_negocio",
  cuenta: "cuenta",
  ot: "ot",
  idasesor: "idasesor",
  nombreasesor: "nombreasesor",
  cantserv: "cantserv",
  tipored: "tipored",
  division: "division",
  area: "area",
  zona: "zona",
  poblacion: "poblacion",

  d_distrito: "d_distrito",

  renta: "renta",
  fecha: "fecha",
  venta: "venta",

  tipo_registro: "tipo_registro",
  estrato: "estrato",
  paquete_pvd: "paquete_pvd",
  mintic: "mintic",

  tipoprodcuto: "tipo_prodcuto",
  ventaconvergente: "ventaconvergente",

  ventainstaledth: "venta_instale_dth",
  sac_final: "sac_final",
  cedula_vendedor: "cedula_vendedor",
  nombre_vendedor: "nombre_vendedor",
  modalidad_venta: "modalidad_venta",
  tipo_vendedor: "tipo_vendedor",
  tiporedcomercial: "tipo_red_comercial",

  nombre_regional: "nombre_regional",
  nombre_comercial: "nombre_comercial",
  nombre_lider: "nombre_lider",
  retencion_control: "retencion_control",
  observ_retencion: "observ_retencion",
  tipo_contrato: "tipo_contrato",
  tarifa_venta: "tarifa_venta",
  comision_neta: "comision_neta",
  punto_equilibrio: "punto_equilibrio"
};


  const client = await pool.connect()
  try {
    await client.query("BEGIN")
    await client.query("TRUNCATE staging.siapp_full")

    let inserted = 0

    for (let i = 2; i <= sheet.rowCount; i++) {
      
      const row = sheet.getRow(i)
      if (!row || row.values.length <= 1) continue

      const values = row.values.slice(1)
      const obj = {}

      values.forEach((v, idx) => {
        const key = normalizedHeaders[idx]
        const col = allowed[key]
        if (!col) return

        // Asignaciones seguras:
        if (col === "cantserv") obj[col] = toNumericSafe(v)
        else if (col === "tarifa_venta") obj[col] = toNumericSafe(v)
        else if (col === "comision_neta") obj[col] = toNumericSafe(v)
        else if (col === "punto_equilibrio") obj[col] = toNumericSafe(v)
        else if (col === "fecha") obj[col] = toDateSafe(v)
        else obj[col] = v == null ? null : String(v)
      })

      await client.query(
        `
        INSERT INTO staging.siapp_full (
          ${Object.values(allowed).join(", ")},
          raw_json,
          source_file
        )
        VALUES (
          ${Object.values(allowed).map((_, i) => `$${i+1}`).join(", ")},
          $${Object.values(allowed).length + 1},
          $${Object.values(allowed).length + 2}
        )
        `,
        [
          ...Object.values(allowed).map(c => obj[c] ?? null),
          obj,
          source_file ?? null
        ]
      )
      
      inserted++



    }

    await client.query("COMMIT")
    return { ok: true, inserted }

  } catch (e) {
    await client.query("ROLLBACK")
    console.error("[IMPORT siapp_full] Error:", e)
    throw e
  } finally {
    client.release()
  }
}

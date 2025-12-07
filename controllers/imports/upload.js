// controllers/imports/upload.js
import fs from 'fs'
import { extractRows, bulkInsertToStaging, truncateStagingTable } from '../../services/imports.service.js'

export async function importDataset(req, res) {
  const { dataset } = req.params
  if (!req.file) return res.status(400).json({ error: 'Archivo requerido (field: file)' })

  try {
    // 1) Parsear  **IMPORTANTE: AWAIT**
    const parsed = await extractRows(req.file.path, dataset)
    if (!parsed) throw new Error('Parser retornó vacío')

    const { rows, table, columns } = parsed
    if (!table) throw new Error('Parser no devolvió tabla')
    if (!Array.isArray(columns) || !Array.isArray(rows)) {
      throw new Error('Parser devolvió columnas/filas inválidas')
    }

    // 2) Limpiar staging (opcional)
    await truncateStagingTable(table)

    // 3) Insertar
    const inserted = await bulkInsertToStaging(table, columns, rows)

    // 4) Borrar archivo temporal
    fs.unlink(req.file.path, () => {})

    return res.json({ dataset, table, inserted, columns })
  } catch (e) {
    console.error('[IMPORT]', e)
    // aseguramos borrar el temporal si algo falló
    if (req.file?.path) fs.unlink(req.file.path, () => {})
    return res.status(400).json({ error: e.message })
  }
}

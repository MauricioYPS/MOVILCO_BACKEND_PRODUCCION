// controllers/imports/nomina.upload.js
import { importNominaFromExcel } from '../../services/imports.nomina.service.js'

export async function importNomina(req, res) {
  try {
    if (!req.file?.buffer) return res.status(400).json({ error: 'Archivo requerido' })
    const result = await importNominaFromExcel(req.file.buffer)
    res.json({
      dataset: 'nomina',
      table: 'staging.archivo_nomina',
      inserted: result.inserted,
      headerRow: result.headerRow,
      columns: result.columns
    })
  } catch (err) {
    console.error('[IMPORT NOMINA] ', err)
    res.status(400).json({ error: err.message || 'Error importando nómina' })
  }
}

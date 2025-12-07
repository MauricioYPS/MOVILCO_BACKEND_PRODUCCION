import { generateConsolidatedReport } from '../../services/reports.consolidated.service.js'

export async function consolidated(req, res) {
  try {
    const { period, scope = 'company', unit_id = null, format = 'xlsx' } = req.query
    if (!period) return res.status(400).json({ error: 'period requerido (YYYY-MM)' })

    const result = await generateConsolidatedReport({
      period,
      scope: String(scope).toLowerCase(),
      unit_id: unit_id ? Number(unit_id) : null,
      format: String(format).toLowerCase()
    })

    res.setHeader('Content-Disposition', `attachment; filename="${result.filename}"`)
    res.setHeader('Content-Type', result.mime)
    return res.status(200).send(result.buffer)
  } catch (err) {
    console.error('[REPORT CONSOLIDATED]', err)
    return res.status(500).json({ error: 'No se pudo generar el consolidado', detail: err.message })
  }
}

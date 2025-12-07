import { parsePeriod, kpiForLevel } from '../../services/kpi.service.js'

export async function getLevelKpi(req, res) {
  try {
    const { unit_type } = req.params
    const period = parsePeriod(req.query.period)
    if (!period) return res.status(400).json({ error: 'Usa ?period=YYYY-MM' })

    const data = await kpiForLevel({ unit_type: String(unit_type).toUpperCase(), year: period.year, month: period.month })
    res.json({ ok: true, items: data, count: data.length, period_year: period.year, period_month: period.month })
  } catch (e) {
    console.error('[KPI level]', e)
    res.status(500).json({ error: 'No se pudo calcular KPI por nivel', detail: e.message })
  }
}

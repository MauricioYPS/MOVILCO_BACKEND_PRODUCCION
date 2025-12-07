import { parsePeriod, kpiForUnit } from '../../services/kpi.service.js'

export async function getUnitKpi(req, res) {
  try {
    const { unit_id } = req.params
    const period = parsePeriod(req.query.period)

    if (!period) {
      return res.status(400).json({ error: 'Usa ?period=YYYY-MM' })
    }

    const data = await kpiForUnit({
      unit_id: Number(unit_id),
      year: period.year,
      month: period.month
    })

    if (!data) {
      return res.status(404).json({ error: 'Unidad no encontrada' })
    }

    return res.json({ ok: true, ...data })

  } catch (e) {
    console.error('[KPI unit]', e)
    return res.status(500).json({
      error: 'No se pudo calcular el KPI de la unidad',
      detail: e.message
    })
  }
}

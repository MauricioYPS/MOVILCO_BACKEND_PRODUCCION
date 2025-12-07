// controllers/promote/siapp.js
import { promoteSiappFromFullSales } from '../../services/promote.siapp.service.js'

export async function promoteSiapp(req, res) {
  try {
    let year, month

    if (req.query.period) {
      const m = String(req.query.period).match(/^(\d{4})-(\d{1,2})$/)
      if (m) { year = Number(m[1]); month = Number(m[2]) }
    }

    if (!year || !month) {
      if (req.query.year && req.query.month) {
        year = Number(req.query.year)
        month = Number(req.query.month)
      }
    }

    if (!year || !month) {
      return res.status(400).json({ error: 'Provee ?period=YYYY-MM o ?year=YYYY&month=MM' })
    }

    // NUEVA FUNCIÓN CORRECTA
    const result = await promoteSiappFromFullSales({
      period_year: year,
      period_month: month
    })

    res.json({
      ok: true,
      period_year: year,
      period_month: month,
      ...result
    })

  } catch (e) {
    console.error('[PROMOTE siapp]', e)
    res.status(500).json({ error: 'No se pudo promover SIAPP', detail: e.message })
  }
}

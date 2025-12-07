// controllers/analytics/progress.js

import { analyticsProgress } from '../../services/analytics.progress.service.js'

// Helper para formatear periodo
function parsePeriod(q) {
  const m = String(q || '').match(/^(\d{4})-(\d{1,2})$/)
  if (!m) return null
  return { year: Number(m[1]), month: Number(m[2]) }
}

/**
 * SUMMARY + DETAIL: ahora ambos endpoints se basan en analyticsProgress()
 * No rompemos rutas antiguas, pero adaptamos la lógica a SIAPP FULL.
 */

export async function progressSummary(req, res) {
  try {
    const { period, unit_id } = req.query

    const per = parsePeriod(period)
    if (!per) {
      return res.status(400).json({ error: 'Periodo inválido. Usa YYYY-MM' })
    }

    const data = await analyticsProgress({
      period,
      unit_id: unit_id ?? null,
      limit: 99999,   // máximo para summary
      offset: 0
    })

    return res.json(data)

  } catch (e) {
    console.error('[ANALYTICS summary]', e)
    return res.status(400).json({ error: e.message })
  }
}


export async function progressByUser(req, res) {
  try {
    const { period, unit_id, limit, offset } = req.query

    const per = parsePeriod(period)
    if (!per) {
      return res.status(400).json({ error: 'Periodo inválido. Usa YYYY-MM' })
    }

    const data = await analyticsProgress({
      period,
      unit_id: unit_id ?? null,
      limit: limit ?? 50,
      offset: offset ?? 0
    })

    return res.json(data)

  } catch (e) {
    console.error('[ANALYTICS rows]', e)
    return res.status(400).json({ error: e.message })
  }
}

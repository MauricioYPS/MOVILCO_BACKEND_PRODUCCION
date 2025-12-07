// controllers/promote/estructura.js
import { promoteEstructuraFromStaging } from '../../services/promote.service.js'

export async function promoteEstructura(req, res) {
  try {
    const info = await promoteEstructuraFromStaging()
    res.json({ ok: true, ...info })
  } catch (e) {
    console.error('[PROMOTE estructura]', e)
    res.status(500).json({ error: 'No se pudo promover la estructura' })
  }
}

import { loadSettings } from '../../services/settings.service.js'

export async function getSettings(req, res) {
  try {
    const s = await loadSettings(false)
    res.json({ ok: true, settings: s })
  } catch (e) {
    res.status(500).json({ error: 'No se pudieron obtener settings', detail: e.message })
  }
}

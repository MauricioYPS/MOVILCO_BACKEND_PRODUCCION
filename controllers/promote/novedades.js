import { promoteNovedadesFromStaging } from '../../services/promote.novedades.service.js'

export async function promoteNovedades(req, res) {
  try {
    const result = await promoteNovedadesFromStaging()
    res.json({ ok: true, ...result })
  } catch (e) {
    console.error('[PROMOTE novedades]', e)
    res.status(500).json({ error: 'No se pudieron promover las novedades', detail: e.message })
  }
}

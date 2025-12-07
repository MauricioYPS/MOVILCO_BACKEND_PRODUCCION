import {
  getUnitById,
  hasUsers,
  deleteUnit
} from '../../services/units.service.js'

export async function remove(req, res) {
  try {
    const id = Number(req.params.id)
    if (Number.isNaN(id)) return res.status(400).json({ error: 'id inválido' })

    const unit = await getUnitById(id)
    if (!unit) return res.status(404).json({ error: 'Unidad no encontrada' })

    if (await hasUsers(id)) {
      return res.status(409).json({
        error: 'No se puede eliminar: existen usuarios asociados a esta unidad'
      })
    }

    await deleteUnit(id)
    res.status(204).send()
  } catch (err) {
    console.error('[DELETE /org/units/:id] DB error:', err)
    res.status(500).json({ error: 'Error al eliminar la unidad' })
  }
}

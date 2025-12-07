import {
  VALID_UNIT_TYPES,
  isValidUnitType,
  getUnitById,
  parentExists,
  wouldCreateCycle,
  updateUnit
} from '../../services/units.service.js'

export async function update(req, res) {
  try {
    const id = Number(req.params.id)
    if (Number.isNaN(id)) return res.status(400).json({ error: 'id inválido' })

    const current = await getUnitById(id)
    if (!current) return res.status(404).json({ error: 'Unidad no encontrada' })

    const payload = {
      name: req.body?.name ?? current.name,
      unit_type: req.body?.unit_type ?? current.unit_type,
      parent_id: req.body?.parent_id === undefined ? current.parent_id : req.body.parent_id
    }

    if (!payload.name || typeof payload.name !== 'string' || payload.name.trim() === '') {
      return res.status(400).json({ error: 'name inválido' })
    }
    if (!isValidUnitType(payload.unit_type)) {
      return res.status(400).json({ error: `unit_type debe ser uno de: ${VALID_UNIT_TYPES.join(', ')}` })
    }
    if (payload.parent_id != null && Number.isNaN(Number(payload.parent_id))) {
      return res.status(400).json({ error: 'parent_id inválido' })
    }
    if (!(await parentExists(payload.parent_id ?? null))) {
      return res.status(400).json({ error: 'parent_id no existe' })
    }
    if (await wouldCreateCycle(id, payload.parent_id ?? null)) {
      return res.status(409).json({ error: 'No se puede asignar ese parent_id: generaría un ciclo' })
    }

    const updated = await updateUnit(id, payload)
    res.json(updated)
  } catch (err) {
    console.error('[PUT /org/units/:id] DB error:', err)
    res.status(500).json({ error: 'Error al actualizar la unidad' })
  }
}

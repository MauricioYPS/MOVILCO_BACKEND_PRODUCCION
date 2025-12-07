import {
  VALID_UNIT_TYPES,
  isValidUnitType,
  parentExists,
  createUnit
} from '../../services/units.service.js'

export async function create(req, res) {
  try {
    const { name, unit_type, parent_id } = req.body

    if (!name || typeof name !== 'string' || name.trim() === '') {
      return res.status(400).json({ error: 'name es requerido' })
    }
    if (!isValidUnitType(unit_type)) {
      return res.status(400).json({
        error: `unit_type debe ser uno de: ${VALID_UNIT_TYPES.join(', ')}`
      })
    }
    if (parent_id != null && Number.isNaN(Number(parent_id))) {
      return res.status(400).json({ error: 'parent_id inválido' })
    }
    if (!(await parentExists(parent_id ?? null))) {
      return res.status(400).json({ error: 'parent_id no existe' })
    }

    const unit = await createUnit({ name, unit_type, parent_id })
    res.status(201).json(unit)
  } catch (err) {
    console.error('[POST /org/units] DB error:', err)
    res.status(500).json({ error: 'Error al crear la unidad' })
  }
}

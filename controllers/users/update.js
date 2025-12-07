import {
  VALID_ROLES, isValidRole,
  getUserById, orgUnitExists, emailInUse, updateUser
} from '../../services/users.service.js'

export async function update(req, res) {
  try {
    const id = Number(req.params.id)
    if (Number.isNaN(id)) return res.status(400).json({ error: 'id inválido' })

    const current = await getUserById(id)
    if (!current) return res.status(404).json({ error: 'Usuario no encontrado' })

    const payload = {
      org_unit_id: req.body?.org_unit_id ?? current.org_unit_id,
      document_id: req.body?.document_id ?? current.document_id,
      advisor_id: req.body?.advisor_id ?? current.advisor_id,
      name: req.body?.name ?? current.name,
      email: req.body?.email ?? current.email,
      phone: req.body?.phone ?? current.phone,
      role: req.body?.role ?? current.role,
      active: req.body?.active ?? current.active
    }

    if (Number.isNaN(Number(payload.org_unit_id))) {
      return res.status(400).json({ error: 'org_unit_id inválido' })
    }
    if (!(await orgUnitExists(payload.org_unit_id))) {
      return res.status(400).json({ error: 'La unidad organizacional no existe' })
    }
    if (!payload.name || typeof payload.name !== 'string' || payload.name.trim() === '') {
      return res.status(400).json({ error: 'name inválido' })
    }
    if (!payload.email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(payload.email))) {
      return res.status(400).json({ error: 'email inválido' })
    }
    if (await emailInUse(String(payload.email).toLowerCase(), id)) {
      return res.status(409).json({ error: 'El email ya está en uso por otro usuario' })
    }
    if (!isValidRole(payload.role)) {
      return res.status(400).json({ error: `role debe ser uno de: ${VALID_ROLES.join(', ')}` })
    }

    const updated = await updateUser(id, payload)
    res.json(updated)
  } catch (e) {
    console.error('[PUT /users/:id] error:', e)
    res.status(500).json({ error: 'Error al actualizar usuario' })
  }
}

import {
  VALID_ROLES, isValidRole,
  orgUnitExists, emailInUse, createUser
} from '../../services/users.service.js'

export async function create(req, res) {
  try {
    const { org_unit_id, name, email, phone, role, document_id, advisor_id } = req.body

    if (Number.isNaN(Number(org_unit_id))) {
      return res.status(400).json({ error: 'org_unit_id inválido' })
    }
    if (!(await orgUnitExists(org_unit_id))) {
      return res.status(400).json({ error: 'La unidad organizacional no existe' })
    }
    if (!name || typeof name !== 'string' || name.trim() === '') {
      return res.status(400).json({ error: 'name es requerido' })
    }
    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(email))) {
      return res.status(400).json({ error: 'email inválido' })
    }
    if (await emailInUse(String(email).toLowerCase())) {
      return res.status(409).json({ error: 'El email ya está en uso' })
    }
    if (!isValidRole(role)) {
      return res.status(400).json({ error: `role debe ser uno de: ${VALID_ROLES.join(', ')}` })
    }

    const user = await createUser({ org_unit_id, document_id, advisor_id, name, email, phone, role })
    res.status(201).json(user)
  } catch (e) {
    console.error('[POST /users] error:', e)
    res.status(500).json({ error: 'Error al crear usuario' })
  }
}

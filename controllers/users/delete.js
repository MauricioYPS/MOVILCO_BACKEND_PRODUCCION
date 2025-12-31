// controllers/users/delete.js
import { getUserById, deactivateUser } from '../../services/users.service.js'

export async function remove(req, res) {
  try {
    const id = Number(req.params.id)
    if (Number.isNaN(id)) return res.status(400).json({ error: 'id inválido' })

    const current = await getUserById(id)
    if (!current) return res.status(404).json({ error: 'Usuario no encontrado' })

    // Soft delete: desactiva (no borra)
    await deactivateUser(id)

    // 204 para mantener compatibilidad con tu front/Postman
    return res.status(204).send()
  } catch (e) {
    console.error('[DELETE /users/:id] error:', e)
    return res.status(500).json({ error: 'Error al desactivar usuario' })
  }
}

import { promotePresupuestoUsuarios } from '../../services/promote.presupuesto_usuarios.service.js'

export async function promotePresupuestoUsuariosController(req, res) {
  try {
    const result = await promotePresupuestoUsuarios()
    res.json(result)
  } catch (e) {
    res.status(400).json({ error: e.message })
  }
}

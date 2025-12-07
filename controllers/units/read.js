import {
  listUnits,
  getUnitById
} from '../../services/units.service.js'

export async function list(req, res) {
  try {
    const rows = await listUnits()
    res.json(rows)
  } catch (err) {
    console.error('[GET /org/units] DB error:', err)
    res.status(500).json({ error: 'Error al listar unidades' })
  }
}

export async function getOne(req, res) {
  try {
    const id = Number(req.params.id)
    if (Number.isNaN(id)) return res.status(400).json({ error: 'id inválido' })

    const unit = await getUnitById(id)
    if (!unit) return res.status(404).json({ error: 'Unidad no encontrada' })

    res.json(unit)
  } catch (err) {
    console.error('[GET /org/units/:id] DB error:', err)
    res.status(500).json({ error: 'Error al obtener la unidad' })
  }
}

export async function tree(req, res) {
  try {
    const rows = await listUnits()
    const map = new Map()
    rows.forEach(u => map.set(u.id, { ...u, children: [] }))
    const roots = []
    rows.forEach(u => {
      const node = map.get(u.id)
      if (u.parent_id == null) roots.push(node)
      else {
        const parent = map.get(u.parent_id)
        if (parent) parent.children.push(node)
        else roots.push(node)
      }
    })
    res.json(roots)
  } catch (err) {
    console.error('[GET /org/units/tree] DB error:', err)
    res.status(500).json({ error: 'Error al construir el árbol' })
  }
}


export async function root(req, res) {
  try {
    const rows = await listUnits()
    const roots = rows.filter(u => u.parent_id === null)
    res.json(roots)
  } catch (err) {
    console.error('[GET /org/units/root] DB error:', err)
    res.status(500).json({ error: 'Error al obtener las unidades raíz' })
  }
}
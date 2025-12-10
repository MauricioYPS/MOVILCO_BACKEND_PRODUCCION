// controllers/users/read.js
import { listUsers, getUserById } from '../../services/users.service.js';
import { fetchBasicForCoordinatorFull } from '../../services/report.payroll.service.js';

export async function list(req, res) {
  try {
    let filter = {};

    if (req.query.orgUnitId) {
      const id = Number(req.query.orgUnitId);
      if (!Number.isNaN(id)) filter.orgUnitIds = [id];

    } else if (req.query.orgUnitIds) {
      const ids = String(req.query.orgUnitIds)
        .split(',')
        .map(x => Number(x.trim()))
        .filter(n => !Number.isNaN(n));
      if (ids.length) filter.orgUnitIds = ids;
    }

    const rows = await listUsers(filter);
    res.json(rows);

  } catch (e) {
    console.error('[GET /users] error:', e);
    res.status(500).json({ error: 'Error al listar usuarios' });
  }
}

export async function getOne(req, res) {
  try {
    const id = Number(req.params.id);
    if (Number.isNaN(id)) {
      return res.status(400).json({ error: 'id invalido' });
    }

    const u = await getUserById(id);
    if (!u) {
      return res.status(404).json({ error: 'Usuario no encontrado' });
    }

    res.json(u);

  } catch (e) {
    console.error('[GET /users/:id] error:', e);
    res.status(500).json({ error: 'Error al obtener usuario' });
  }
}

export async function getAdvisorsByCoordinator(req, res) {
  try {
    const { id } = req.params;
    const { period } = req.query;

    if (!period) {
      return res.status(400).json({ error: 'Falta el periodo YYYY-MM' });
    }

    const match = period.match(/^(\d{4})-(\d{2})$/);
    if (!match) {
      return res.status(400).json({ error: 'Periodo invalido' });
    }

    const year = Number(match[1]);
    const month = Number(match[2]);

    const advisors = await fetchBasicForCoordinatorFull({
      period_year: year,
      period_month: month,
      coordinator_id: Number(id)
    });

    return res.json(advisors);

  } catch (err) {
    console.error('[GET /users/coordinator/:id]', err);
    res.status(500).json({ error: 'Error cargando asesores del coordinador' });
  }
}

export async function getUsersByDirector(req, res) {
  try {
    const directorId = Number(req.params.id);

    // 1. Cargar todos los usuarios (un solo consumo, como pediste)
    const allUsers = await listUsers();

    // 2. Encontrar al director
    const director = allUsers.find(u => Number(u.user_id) === directorId);

    if (!director) {
      return res.status(404).json({
        ok: false,
        error: "Director no encontrado"
      });
    }

    // 3. Tomar su dirección (clave principal)
    const direccionParentId = director.direccion_parent_id;

    if (!direccionParentId) {
      return res.status(400).json({
        ok: false,
        error: "El usuario no pertenece a ninguna dirección"
      });
    }

    // 4. Filtrar todo lo que pertenece a la misma dirección
    const usuariosDireccion = allUsers.filter(u =>
      u.direccion_parent_id &&
      String(u.direccion_parent_id) === String(direccionParentId)
    );

    // 5. Filtrar solo asesores si quieres (opcional)
    const asesores = usuariosDireccion.filter(u =>
      u.role === "ASESORIA" || u.jerarquia === "ASESORIA"
    );

    return res.json({
      ok: true,
      director,
      direccion_parent_id: direccionParentId,
      total_usuarios: usuariosDireccion.length,
      total_asesores: asesores.length,
      usuarios: usuariosDireccion,
      asesores
    });

  } catch (error) {
    console.error("getUsersByDirector ERROR:", error);
    return res.status(500).json({
      ok: false,
      error: "Error interno del servidor",
      detail: error.message
    });
  }
}


export async function getCoordinadoresByDirector(req, res) {
  try {
    const directorId = Number(req.params.id);

    // 1. Consumir toda la lista (solo una vez)
    const allUsers = await listUsers();

    // 2. Encontrar al director
    const director = allUsers.find(u => Number(u.user_id) === directorId);

    if (!director) {
      return res.status(404).json({
        ok: false,
        error: "Director no encontrado"
      });
    }

    // 3. Identificar el direccion_parent_id
    const direccionParentId = director.direccion_parent_id;

    if (!direccionParentId) {
      return res.status(400).json({
        ok: false,
        error: "El usuario no pertenece a ninguna dirección"
      });
    }

    // 4. Filtrar usuarios dentro de la misma dirección
    const usuariosDireccion = allUsers.filter(u =>
      u.direccion_parent_id &&
      String(u.direccion_parent_id) === String(direccionParentId)
    );

    // 5. Filtrar coordinadores
    const coordinadores = usuariosDireccion.filter(u =>
      u.role === "COORDINACION" ||
      u.jerarquia === "COORDINACION"
    );

    return res.json({
      ok: true,
      director,
      direccion_parent_id: direccionParentId,
      total_coordinadores: coordinadores.length,
      coordinadores
    });

  } catch (error) {
    console.error("getCoordinadoresByDirector ERROR:", error);
    return res.status(500).json({
      ok: false,
      error: "Error interno del servidor",
      detail: error.message
    });
  }
}

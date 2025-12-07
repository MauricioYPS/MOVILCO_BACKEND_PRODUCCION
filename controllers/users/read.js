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
    const id = Number(req.params.document_id);
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

// controllers/users/update.js
import {
  VALID_ROLES,
  isValidRole,
  getUserById,
  getCoordinatorById,
  orgUnitExists,
  emailInUse,
  updateUser
} from "../../services/users.service.js";

function isAsesoria({ role, jerarquia }) {
  return (
    String(role || "").toUpperCase() === "ASESORIA" ||
    String(jerarquia || "").toUpperCase() === "ASESORIA"
  );
}

export async function update(req, res) {
  try {
    const id = Number(req.params.id);
    if (Number.isNaN(id)) return res.status(400).json({ error: "id inválido" });

    const current = await getUserById(id);
    if (!current) return res.status(404).json({ error: "Usuario no encontrado" });

    // Merge seguro
    const payload = {
      org_unit_id: req.body?.org_unit_id ?? current.org_unit_id,
      document_id: req.body?.document_id ?? current.document_id,
      advisor_id: req.body?.advisor_id ?? current.advisor_id,
      coordinator_id: req.body?.coordinator_id ?? current.coordinator_id,

      name: req.body?.name ?? current.name,
      email: req.body?.email ?? current.email,
      phone: req.body?.phone ?? current.phone,
      role: req.body?.role ?? current.role,
      active: req.body?.active ?? current.active,

      district: req.body?.district ?? current.district,
      district_claro: req.body?.district_claro ?? current.district_claro,

      regional: req.body?.regional ?? current.regional,
      cargo: req.body?.cargo ?? current.cargo,
      capacity: req.body?.capacity ?? current.capacity,
      jerarquia: req.body?.jerarquia ?? current.jerarquia,
      contract_start: req.body?.contract_start ?? current.contract_start,
      contract_end: req.body?.contract_end ?? current.contract_end,
      notes: req.body?.notes ?? current.notes
    };

    // Validaciones base
    if (!payload.name || typeof payload.name !== "string" || payload.name.trim() === "") {
      return res.status(400).json({ error: "name inválido" });
    }

    if (!payload.email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(payload.email))) {
      return res.status(400).json({ error: "email inválido" });
    }

    const emailLower = String(payload.email).toLowerCase().trim();
    if (await emailInUse(emailLower, id)) {
      return res.status(409).json({ error: "El email ya está en uso por otro usuario" });
    }

    if (!isValidRole(payload.role)) {
      return res.status(400).json({ error: `role debe ser uno de: ${VALID_ROLES.join(", ")}` });
    }

    // Normalizaciones
    payload.name = payload.name.trim();
    payload.email = emailLower;
    payload.role = String(payload.role).toUpperCase();
    payload.jerarquia = payload.jerarquia != null ? String(payload.jerarquia).toUpperCase() : payload.jerarquia;

    // --- IMPOSICIÓN DE HERENCIA PARA ASESORIA ---
    const asesoria = isAsesoria({ role: payload.role, jerarquia: payload.jerarquia });

    if (asesoria) {
      const coordIdNum = Number(payload.coordinator_id);
      if (!coordIdNum || Number.isNaN(coordIdNum)) {
        return res.status(400).json({ error: "coordinator_id es obligatorio para ASESORIA" });
      }

      const coord = await getCoordinatorById(coordIdNum);
      if (!coord) return res.status(400).json({ error: "coordinator_id no existe" });
      if (!coord._isCoordinator) return res.status(400).json({ error: "El usuario indicado no es COORDINACION" });
      if (coord.active !== true) return res.status(400).json({ error: "El coordinador está inactivo" });

      // Sobrescribir “heredados” sin importar lo que venga en body
      payload.coordinator_id = coord.id;
      payload.org_unit_id = coord.org_unit_id;
      payload.regional = coord.regional ?? null;
      payload.district = coord.district ?? null;
      payload.district_claro = coord.district_claro ?? null;
    }

    // org_unit_id siempre debe ser válido (si asesoria ya viene del coordinador)
    if (Number.isNaN(Number(payload.org_unit_id))) {
      return res.status(400).json({ error: "org_unit_id inválido" });
    }
    if (!(await orgUnitExists(payload.org_unit_id))) {
      return res.status(400).json({ error: "La unidad organizacional no existe" });
    }

    const updated = await updateUser(id, payload);
    return res.json(updated);
  } catch (e) {
    console.error("[PUT /users/:id] error:", e);
    return res.status(500).json({ error: "Error al actualizar usuario" });
  }
}

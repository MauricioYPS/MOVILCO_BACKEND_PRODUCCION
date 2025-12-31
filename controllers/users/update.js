// controllers/users/update.js
import {
  VALID_ROLES,
  isValidRole,
  getUserById,
  orgUnitExists,
  emailInUse,
  updateUser
} from "../../services/users.service.js";

export async function update(req, res) {
  try {
    const id = Number(req.params.id);
    if (Number.isNaN(id)) return res.status(400).json({ error: "id inválido" });

    const current = await getUserById(id);
    if (!current) return res.status(404).json({ error: "Usuario no encontrado" });

    // Merge seguro: si el body NO trae el campo, se conserva el actual.
    // Si el body trae explícitamente null, se guardará null (eso es útil para "limpiar" campos).
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

      // IMPORTANTES (antes no estaban)
      district: req.body?.district ?? current.district,
      district_claro: req.body?.district_claro ?? current.district_claro,

      // Opcionales adicionales (si los estás usando)
      regional: req.body?.regional ?? current.regional,
      cargo: req.body?.cargo ?? current.cargo,
      capacity: req.body?.capacity ?? current.capacity,
      jerarquia: req.body?.jerarquia ?? current.jerarquia,
      contract_start: req.body?.contract_start ?? current.contract_start,
      contract_end: req.body?.contract_end ?? current.contract_end,
      notes: req.body?.notes ?? current.notes
    };

    if (Number.isNaN(Number(payload.org_unit_id))) {
      return res.status(400).json({ error: "org_unit_id inválido" });
    }
    if (!(await orgUnitExists(payload.org_unit_id))) {
      return res.status(400).json({ error: "La unidad organizacional no existe" });
    }
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

    // Normalizaciones básicas
    payload.name = payload.name.trim();
    payload.email = emailLower;
    payload.role = String(payload.role).toUpperCase();

    const updated = await updateUser(id, payload);
    return res.json(updated);
  } catch (e) {
    console.error("[PUT /users/:id] error:", e);
    return res.status(500).json({ error: "Error al actualizar usuario" });
  }
}

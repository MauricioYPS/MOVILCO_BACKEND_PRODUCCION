// controllers/users/create.js
import bcrypt from "bcrypt";

import {
  VALID_ROLES,
  isValidRole,
  orgUnitExists,
  emailInUse,
  createUser,
  getCoordinatorById
} from "../../services/users.service.js";

function isAsesoria({ role, jerarquia }) {
  return (
    String(role || "").toUpperCase() === "ASESORIA" ||
    String(jerarquia || "").toUpperCase() === "ASESORIA"
  );
}

export async function create(req, res) {
  try {
    const {
      org_unit_id,
      name,
      email,
      phone,
      role,
      document_id,
      advisor_id,
      coordinator_id = null,

      district = null,
      district_claro = null,
      regional = null,
      cargo = null,
      capacity = null,
      jerarquia = null,
      contract_start = null,
      contract_end = null,
      notes = null,
      active = true,

      password = null
    } = req.body;

    // Validaciones base
    if (!name || typeof name !== "string" || name.trim() === "") {
      return res.status(400).json({ error: "name es requerido" });
    }

    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(email))) {
      return res.status(400).json({ error: "email inválido" });
    }

    const emailLower = String(email).toLowerCase().trim();
    if (await emailInUse(emailLower)) {
      return res.status(409).json({ error: "El email ya está en uso" });
    }

    if (!isValidRole(role)) {
      return res.status(400).json({
        error: `role debe ser uno de: ${VALID_ROLES.join(", ")}`
      });
    }

    // Hash opcional de password (si lo envían desde admin)
    let password_hash = null;
    if (password != null && String(password).trim() !== "") {
      password_hash = await bcrypt.hash(String(password), 10);
    }

    // --- IMPOSICIÓN DE HERENCIA PARA ASESORIA ---
    const asesoria = isAsesoria({ role, jerarquia });

    let final_org_unit_id = org_unit_id;
    let final_coordinator_id = coordinator_id;
    let final_regional = regional;
    let final_district = district;
    let final_district_claro = district_claro;

    if (asesoria) {
      const coordIdNum = Number(coordinator_id);
      if (!coordIdNum || Number.isNaN(coordIdNum)) {
        return res.status(400).json({ error: "coordinator_id es obligatorio para ASESORIA" });
      }

      const coord = await getCoordinatorById(coordIdNum);
      if (!coord) return res.status(400).json({ error: "coordinator_id no existe" });
      if (!coord._isCoordinator) return res.status(400).json({ error: "El usuario indicado no es COORDINACION" });
      if (coord.active !== true) return res.status(400).json({ error: "El coordinador está inactivo" });

      final_coordinator_id = coord.id;
      final_org_unit_id = coord.org_unit_id;
      final_regional = coord.regional ?? null;
      final_district = coord.district ?? null;
      final_district_claro = coord.district_claro ?? null;
    }

    // org_unit_id debe existir (si no es asesoria, viene del body; si es asesoria, viene del coordinador)
    if (Number.isNaN(Number(final_org_unit_id))) {
      return res.status(400).json({ error: "org_unit_id inválido" });
    }
    if (!(await orgUnitExists(final_org_unit_id))) {
      return res.status(400).json({ error: "La unidad organizacional no existe" });
    }

    const user = await createUser({
      org_unit_id: Number(final_org_unit_id),
      document_id: document_id ?? null,
      advisor_id: advisor_id ?? null,
      coordinator_id: final_coordinator_id ?? null,

      name: name.trim(),
      email: emailLower,
      phone: phone ?? null,
      role: String(role).toUpperCase(),
      active: active !== undefined ? !!active : true,

      // heredados si ASESORIA
      district: final_district ?? null,
      district_claro: final_district_claro ?? null,
      regional: final_regional ?? null,

      cargo: cargo ?? null,
      capacity: capacity ?? null,
      jerarquia: jerarquia ?? null,
      contract_start: contract_start ?? null,
      contract_end: contract_end ?? null,
      notes: notes ?? null,

      password_hash
    });

    return res.status(201).json(user);
  } catch (e) {
    console.error("[POST /users] error:", e);
    return res.status(500).json({ error: "Error al crear usuario" });
  }
}

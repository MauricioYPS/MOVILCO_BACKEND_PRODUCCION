// controllers/users/create.js
import bcrypt from "bcrypt";

import {
  VALID_ROLES,
  isValidRole,
  orgUnitExists,
  emailInUse,
  createUser
} from "../../services/users.service.js";

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

      // NUEVOS (antes no se guardaban)
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

      // opcional: si lo mandas, lo hasheamos
      password = null
    } = req.body;

    if (Number.isNaN(Number(org_unit_id))) {
      return res.status(400).json({ error: "org_unit_id inválido" });
    }
    if (!(await orgUnitExists(org_unit_id))) {
      return res.status(400).json({ error: "La unidad organizacional no existe" });
    }

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

    const user = await createUser({
      org_unit_id: Number(org_unit_id),
      document_id: document_id ?? null,
      advisor_id: advisor_id ?? null,
      coordinator_id: coordinator_id ?? null,
      name: name.trim(),
      email: emailLower,
      phone: phone ?? null,
      role: String(role).toUpperCase(),
      active: active !== undefined ? !!active : true,

      district: district ?? null,
      district_claro: district_claro ?? null,
      regional: regional ?? null,
      cargo: cargo ?? null,
      capacity: capacity ?? null,
      jerarquia: jerarquia ?? null,
      contract_start: contract_start ?? null,
      contract_end: contract_end ?? null,
      notes: notes ?? null,

      password_hash // puede ser null
    });

    return res.status(201).json(user);
  } catch (e) {
    console.error("[POST /users] error:", e);
    return res.status(500).json({ error: "Error al crear usuario" });
  }
}

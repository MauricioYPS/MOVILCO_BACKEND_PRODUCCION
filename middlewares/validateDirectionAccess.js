import pool from "../config/database.js";

export async function validateDirectionAccess(req, res, next) {
  try {
    const usuario = req.user;
    const dirId = Number(req.params.id);

    // Gerencia puede ver todo
    if (usuario.role === "GERENCIA") return next();

    // Si no es dirección → no tiene permiso
    if (usuario.role !== "DIRECCION") {
      return res.status(403).json({
        ok: false,
        error: "No estás autorizado para ver esta dirección"
      });
    }

    // Solo puede ver SU dirección
    if (usuario.org_unit_id !== dirId) {
      return res.status(403).json({
        ok: false,
        error: "No puedes ver una dirección que no es tuya"
      });
    }

    next();
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: "Error validando dirección" });
  }
}

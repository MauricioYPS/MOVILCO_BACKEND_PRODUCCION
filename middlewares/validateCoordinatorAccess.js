import pool from "../config/database.js";

export async function validateCoordinatorAccess(req, res, next) {
  try {
    const usuario = req.user;

    if (usuario.role !== "COORDINADOR")
      return res.status(403).json({ ok: false, error: "Solo coordinadores pueden acceder" });

    const coordId = Number(req.params.id);

    // Verificamos que el coordinador autenticado sea el mismo de la URL
    if (coordId !== usuario.id) {
      return res.status(403).json({
        ok: false,
        error: "No puedes ver asesores de otro coordinador"
      });
    }

    next();
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: "Error validando acceso" });
  }
}

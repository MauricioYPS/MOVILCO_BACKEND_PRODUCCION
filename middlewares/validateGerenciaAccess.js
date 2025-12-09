export function validateGerenciaAccess(req, res, next) {
  try {
    if (req.user.role !== "GERENCIA") {
      return res.status(403).json({
        ok: false,
        error: "Acceso exclusivo para Gerencia"
      });
    }

    next();
  } catch (err) {
    console.error(err);
    return res.status(500).json({ ok: false, error: "Error en validación de gerencia" });
  }
}

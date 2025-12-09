export function allowRoles(...roles) {
  return (req, res, next) => {
    try {
      if (!req.user || !req.user.role)
        return res.status(401).json({ ok: false, error: "No autenticado" });

      if (!roles.includes(req.user.role))
        return res.status(403).json({ ok: false, error: "Acceso denegado (rol no permitido)" });

      next();
    } catch (err) {
      return res.status(500).json({ ok: false, error: "Error de autorización" });
    }
  };
}

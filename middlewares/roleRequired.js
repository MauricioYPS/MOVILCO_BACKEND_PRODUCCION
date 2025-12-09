export function roleRequired(...allowedRoles) {
  return (req, res, next) => {
    try {
      if (!req.user)
        return res.status(401).json({ ok: false, error: "No autenticado" });

      if (!allowedRoles.includes(req.user.role))
        return res.status(403).json({ ok: false, error: "Acceso denegado" });

      next();

    } catch (err) {
      console.error("[roleRequired]", err);
      return res.status(500).json({ ok: false, error: "Error interno" });
    }
  };
}

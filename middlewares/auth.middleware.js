import jwt from "jsonwebtoken";

const JWT_SECRET = process.env.JWT_SECRET;

export function authRequired(req, res, next) {
  const token = req.headers.authorization?.split(" ")[1];

  if (!token) return res.status(401).json({ ok: false, error: "Token requerido" });

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    req.user = decoded;
    next();
  } catch (err) {
    return res.status(403).json({ ok: false, error: "Token inválido" });
  }
}

export function requireRole(...roles) {
  return (req, res, next) => {
    if (!roles.includes(req.user.role))
      return res.status(403).json({ ok: false, error: "Acceso denegado" });
    next();
  };
}

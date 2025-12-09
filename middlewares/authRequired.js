import jwt from "jsonwebtoken";

export function authRequired(req, res, next) {
  try {
    const header = req.headers.authorization;
    if (!header)
      return res.status(401).json({ ok: false, error: "Token no enviado" });

    const token = header.split(" ")[1];
    if (!token)
      return res.status(401).json({ ok: false, error: "Token inválido" });

    const decoded = jwt.verify(token, process.env.JWT_SECRET);

    // Guardamos los datos del usuario en req.user
    req.user = decoded;

    next();
  } catch (err) {
    if (err.name === "TokenExpiredError") {
      return res.status(401).json({ ok: false, error: "Token expirado" });
    }

    return res.status(401).json({ ok: false, error: "Token inválido" });
  }
}

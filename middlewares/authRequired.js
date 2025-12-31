import jwt from "jsonwebtoken";
import pool from "../config/database.js";

export async function authRequired(req, res, next) {
  try {
    const header = req.headers.authorization;
    if (!header)
      return res.status(401).json({ ok: false, error: "Token no enviado" });

    const token = header.split(" ")[1];
    if (!token)
      return res.status(401).json({ ok: false, error: "Token inválido" });

    // 1. Verificar token
    const decoded = jwt.verify(token, process.env.JWT_SECRET);

    // 2. Validar usuario en BD (estado activo)
    const { rows } = await pool.query(
      `
      SELECT id, active
      FROM core.users
      WHERE id = $1
      LIMIT 1
      `,
      [decoded.id]
    );

    const user = rows[0];

    if (!user) {
      return res.status(401).json({
        ok: false,
        error: "Usuario no existe"
      });
    }

    if (user.active === false) {
      return res.status(403).json({
        ok: false,
        code: "USER_INACTIVE",
        error:
          "Tu cuenta está inactiva. El acceso a la plataforma ha sido restringido. Comunícate con Recursos Humanos."
      });
    }

    // 3. Usuario válido → pasar datos al request
    req.user = decoded;

    next();
  } catch (err) {
    if (err.name === "TokenExpiredError") {
      return res.status(401).json({ ok: false, error: "Token expirado" });
    }

    return res.status(401).json({ ok: false, error: "Token inválido" });
  }
}

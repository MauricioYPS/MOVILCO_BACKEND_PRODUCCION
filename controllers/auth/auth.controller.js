import pool from "../../config/database.js";
import bcrypt from "bcrypt";
import { generateToken } from "../../utils/generateToken.js";

export async function login(req, res) {
  try {
    const { email, password } = req.body;

    if (!email || !password)
      return res.status(400).json({ ok: false, error: "Email y contrasena requeridos" });

    // Buscar usuario
    const q = `
      SELECT id, name, email, password_hash, org_unit_id, document_id, role
      FROM core.users
      WHERE email = $1
      LIMIT 1
    `;
    const { rows } = await pool.query(q, [email]);
    const user = rows[0];

    if (!user)
      return res.status(404).json({ ok: false, error: "Usuario no encontrado" });

    // Si no tiene contrasena guardada, avisar que debe registrarla
    if (!user.password_hash) {
      return res.status(409).json({
        ok: false,
        error: "Usuario registrado en la nomina pero sin contrasena. Cree una contrasena para su correo."
      });
    }

    // Validacion de contrasena
    const isValid = await bcrypt.compare(password, user.password_hash);
    if (!isValid)
      return res.status(401).json({ ok: false, error: "Contrasena incorrecta" });

    // Generar token con 4 horas de duracion
    const token = generateToken({
      id: user.id,
      email: user.email,
      role: user.role,
      org_unit_id: user.org_unit_id
    });

    return res.json({
      ok: true,
      message: "Inicio de sesion exitoso",
      token,
      user: {
        id: user.id,
        name: user.name,
        email: user.email,
        org_unit_id: user.org_unit_id,
        document_id: user.document_id,
        role: user.role
      }
    });

  } catch (err) {
    console.error("[LOGIN ERROR]", err);
    return res.status(500).json({ ok: false, error: "Error interno del servidor" });
  }
}

export async function register(req, res) {
  try {
    const { email, password } = req.body;

    if (!email || !password)
      return res.status(400).json({ ok: false, error: "Email y contrasena son requeridos" });

    // 1. Buscar usuario por email
    const q = `
      SELECT id, name, email, password_hash, org_unit_id, document_id, role
      FROM core.users
      WHERE email = $1
      LIMIT 1
    `;
    const { rows } = await pool.query(q, [email]);
    const user = rows[0];

    if (!user)
      return res.status(404).json({ ok: false, error: "El email no esta registrado en la nomina" });

    // 2. Si ya tiene contrasena -> NO puede volver a registrarse
    if (user.password_hash)
      return res.status(409).json({ ok: false, error: "El usuario ya tiene contrasena. Inicia sesion." });

    // 3. Crear hash
    const saltRounds = 10;
    const hash = await bcrypt.hash(password, saltRounds);

    // 4. Guardar contrasena
    const updateQuery = `
      UPDATE core.users
      SET password_hash = $1
      WHERE id = $2
      RETURNING id, name, email, org_unit_id, document_id, role
    `;

    const updated = await pool.query(updateQuery, [hash, user.id]);
    const updatedUser = updated.rows[0];

    // 5. Generar token para iniciar sesion automatico
    const token = generateToken({
      id: updatedUser.id,
      email: updatedUser.email,
      role: updatedUser.role,
      org_unit_id: updatedUser.org_unit_id
    });

    return res.json({
      ok: true,
      message: "Registro exitoso. Sesion iniciada.",
      token,
      user: updatedUser
    });

  } catch (err) {
    console.error("[REGISTER ERROR]", err);
    return res.status(500).json({ ok: false, error: "Error interno del servidor" });
  }
}

export async function logout(req, res) {
  try {
    // Si usas cookies con token, se limpia asi:
    res.clearCookie("auth_token", {
      httpOnly: true,
      secure: true,
      sameSite: "none"
    });

    return res.json({
      ok: true,
      message: "Sesion cerrada correctamente."
    });

  } catch (error) {
    console.error("[AUTH LOGOUT] Error:", error);
    return res.status(500).json({
      ok: false,
      error: "Error al cerrar sesion"
    });
  }
}
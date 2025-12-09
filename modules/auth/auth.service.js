import pool from "../../config/database.js";
import bcrypt from "bcrypt";
import jwt from "jsonwebtoken";

const JWT_SECRET = process.env.JWT_SECRET || "super_secret_key";

/**
 * REGISTRO — Activación de cuenta
 * Solo requiere email + password
 * Solo si el usuario YA existe
 */
export async function activateAccount({ email, password }) {
  email = String(email).trim().toLowerCase();

  const qUser = `
    SELECT id, name, email, password_hash, role, org_unit_id
    FROM core.users
    WHERE LOWER(email) = $1
  `;
  const { rows } = await pool.query(qUser, [email]);
  const user = rows[0];

  if (!user) throw new Error("Este correo no existe en la nómina.");

  if (user.password_hash)
    throw new Error("Esta cuenta ya ha sido activada. Inicie sesión.");

  const hash = await bcrypt.hash(password, 10);

  await pool.query(
    `UPDATE core.users SET password_hash = $1 WHERE id = $2`,
    [hash, user.id]
  );

  return {
    id: user.id,
    name: user.name,
    email: user.email,
    role: user.role,
    org_unit_id: user.org_unit_id,
  };
}


/**
 * LOGIN
 */
export async function loginUser({ email, password }) {
  email = String(email).trim().toLowerCase();

  const q = `
    SELECT id, name, email, password_hash, role, org_unit_id
    FROM core.users
    WHERE LOWER(email) = $1
  `;
  const { rows } = await pool.query(q, [email]);
  const user = rows[0];

  if (!user) throw new Error("Usuario no encontrado.");

  if (!user.password_hash)
    throw new Error("La cuenta no ha sido activada. Regístrese primero.");

  const match = await bcrypt.compare(password, user.password_hash);
  if (!match) throw new Error("Contraseña incorrecta.");

  const token = jwt.sign(
    {
      id: user.id,
      role: user.role,
      org_unit_id: user.org_unit_id,
    },
    JWT_SECRET,
    { expiresIn: "8h" }
  );

  delete user.password_hash;

  return { user, token };
}

import pool from "../config/database.js";

/** 
 * Obtiene una plantilla por su "codigo".
 */
export async function getEmailTemplateByCode(codigo) {
  const { rows } = await pool.query(
    `SELECT id, codigo, nombre, asunto, html
     FROM core.email_templates
     WHERE codigo = $1
     LIMIT 1`,
    [codigo]
  );

  return rows[0] || null;
}

/**
 * Reemplaza placeholders {{key}} por los valores en data.
 */
export function applyPlaceholders(html, data = {}) {
  let final = html;

  for (const key of Object.keys(data)) {
    const exp = new RegExp(`{{\\s*${key}\\s*}}`, "g");
    final = final.replace(exp, data[key]);
  }

  return final;
}

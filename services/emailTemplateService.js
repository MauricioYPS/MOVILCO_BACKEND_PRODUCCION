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
export function fillEmailTemplate(html, data = {}) {
  let output = html;

  // Reemplazo dinámico
  for (const [key, value] of Object.entries(data)) {
    output = output.replaceAll(`{${key}}`, String(value ?? ""));
  }

  // Valores por defecto OBLIGATORIOS (anti-bloqueo Gmail)
  const defaults = {
    CIUDAD: "—",
    FECHA_COMPLETA: new Date().toLocaleDateString("es-CO", {
      year: "numeric",
      month: "long",
      day: "numeric"
    }),
    GERENTE_NOMBRE: "Gerencia Comercial",
    GERENTE_CARGO: "Gerente Comercial",
  };

  for (const [key, value] of Object.entries(defaults)) {
    output = output.replaceAll(`{${key}}`, value);
  }

  // Validación final: no deben quedar placeholders
  if (/{[A-Z0-9_]+}/.test(output)) {
    throw new Error("La plantilla contiene placeholders sin resolver");
  }

  return output;
}

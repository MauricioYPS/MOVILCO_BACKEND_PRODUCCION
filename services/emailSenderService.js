import pool from "../config/database.js";
import { transporter } from "./emailTransporter.js";
import { getEmailTemplateByCode, applyPlaceholders } from "./emailTemplateService.js";

/**
 * Guarda resultado en email_log.
 */
async function logEmail({ 
  user_id = null,
  template_id,
  email_to,
  asunto,
  html_enviado,
  estado,
  error_message = null,
  payload = null,
  periodo = null
}) {
  await pool.query(
    `INSERT INTO core.email_log (
        user_id,
        template_id,
        email_to,
        asunto,
        html_enviado,
        estado,
        error,
        periodo,
        status,
        error_message,
        payload
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
    [
      user_id,
      template_id,
      email_to,
      asunto,
      html_enviado,
      estado,
      error_message,
      periodo,
      estado,
      error_message,
      payload ? JSON.stringify(payload) : null
    ]
  );
}


/**
 * Envía un correo único basado en una plantilla.
 */
export async function sendEmailTemplate({ codigo, to, data = {}, user_id = null, periodo = null }) {
  const template = await getEmailTemplateByCode(codigo);

  if (!template) {
    throw new Error(`Plantilla no encontrada: ${codigo}`);
  }

  const htmlFinal = applyPlaceholders(template.html, data);

  try {
    const info = await transporter.sendMail({
      from: "yepes060@gmail.com",
      to,
      subject: template.asunto,
      html: htmlFinal
    });

    await logEmail({
      user_id: user_id ?? 0,       // o el user_id real si lo tienes
      template_id: template.id,
      email_to: to,
      asunto: template.asunto,
      html_enviado: htmlFinal,
      estado: "SENT",
      error_message: null,
      payload: data,
      periodo: periodo
    });

    return { ok: true, info };
  } catch (err) {

    await logEmail({
      user_id: user_id ?? 0,
      template_id: template.id,
      email_to: to,
      asunto: template.asunto,
      html_enviado: htmlFinal,
      estado: "ERROR",
      error_message: err.message,
      payload: data,
      periodo: periodo
    });

    return { ok: false, error: err.message };
  }
}


/**
 * Envío múltiple seguro (batch), evitando bloqueo de Gmail.
 */
export async function sendEmailBatch({ codigo, recipients, data, user_id = null, periodo = null }) {
  const results = [];

  for (const email of recipients) {
    const result = await sendEmailTemplate({
      codigo,
      to: email,
      data,
      user_id,
      periodo
    });

    results.push({ email, ...result });

    // Delay recomendado para evitar bloqueo por Gmail
    await new Promise(res => setTimeout(res, 1500));
  }

  return results;
}


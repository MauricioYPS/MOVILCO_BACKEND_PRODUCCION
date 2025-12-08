// modules/mail/mail.service.js
import nodemailer from "nodemailer";
import pool from "../../config/database.js";
import { renderTemplate } from "./render-template.js";

/* --------------------------------------------------------
 * CONFIG SMTP
 * -------------------------------------------------------- */
const transporter = nodemailer.createTransport({
  host: process.env.MAIL_HOST,
  port: process.env.MAIL_PORT,
  secure: false,
  auth: {
    user: process.env.MAIL_USER,
    pass: process.env.MAIL_PASS,
  },
  tls: { rejectUnauthorized: false }
});

const BULK_RETRY_LIMIT = 3;
const BULK_THROTTLE_MS = 250;

/* --------------------------------------------------------
 * UTILIDAD: obtener plantilla por código
 * -------------------------------------------------------- */
export async function getTemplateByCode(code) {
  const q = `
    SELECT id, codigo, nombre, asunto, html
    FROM core.email_templates
    WHERE codigo = $1
  `;
  const { rows } = await pool.query(q, [code]);

  if (!rows.length) throw new Error(`No existe plantilla con código ${code}`);

  return rows[0];
}

/* --------------------------------------------------------
 * UTILIDAD: registrar en email_log
 * -------------------------------------------------------- */
async function logEmail({
  userId,
  templateId,
  to,
  subject,
  html,
  estado,
  errorMsg = null,
  periodo = null,
  payload = null
}) {
  const q = `
    INSERT INTO core.email_log
      (user_id, template_id, email_to, asunto, html_enviado, estado, error, periodo, payload)
    VALUES ($1,       $2,         $3,       $4,     $5,            $6,     $7,    $8,      $9)
  `;

  await pool.query(q, [
    userId,
    templateId,
    to,
    subject,
    html,
    estado,
    errorMsg,
    periodo,
    payload
  ]);
}

/* --------------------------------------------------------
 * ENVÍO INDIVIDUAL (sendTemplateEmail)
 * -------------------------------------------------------- */
export async function sendTemplateEmail({
  templateCode,
  to,
  data = {},
  userId,
  period = null
}) {
  try {
    const template = await getTemplateByCode(templateCode);

    const subject = renderTemplate(template.asunto, data);
    const html = renderTemplate(template.html, data);

    await transporter.sendMail({
      from: `"MOVILCO – Grupo Empresarial" <noreply@devmauricioy.com>`,
      to,
      subject,
      html
    });

    await logEmail({
      userId,
      templateId: template.id,
      to,
      subject,
      html,
      estado: "ENVIADO",
      periodo: period,
      payload: data,
    });

    return {
      ok: true,
      message: "Correo enviado correctamente",
      templateCode,
      templateName: template.nombre,
      asunto_generado: subject,
      data,
      userId,
      email: to,
      preview_html: html
    };

  } catch (err) {
    console.error("[Mail Service] Error individual:", err);

    await logEmail({
      userId,
      templateId: null,
      to,
      subject: "",
      html: "",
      estado: "ERROR",
      errorMsg: err.message,
      periodo: period,
      payload: data
    });

    return {
      ok: false,
      error: err.message,
      templateCode,
      data,
      userId,
      email: to
    };
  }
}

/* --------------------------------------------------------
 * ENVÍO MASIVO VERSIÓN COMPATIBLE CON TU IMPLEMENTACIÓN
 * -------------------------------------------------------- */
export async function sendBulkTemplate({ templateCode, usersList = [], period }) {
  const results = [];

  for (const u of usersList) {
    const { email, user_id, variables } = u;

    const result = await sendTemplateEmail({
      templateCode,
      to: email,
      data: variables,
      userId: user_id,
      period
    });

    results.push({
      userId: user_id,
      email,
      status: result.ok ? "ENVIADO" : "ERROR",
      detalle: result.ok ? null : result.error,
      asunto: result.asunto_generado || null
    });
  }

  return results;
}

/* --------------------------------------------------------
 * ENVÍO MASIVO AVANZADO (sendBulkEmails)
 * -------------------------------------------------------- */
export async function sendBulkEmails({ templateCode, userIds, data }) {
  const results = [];

  // 1. Obtener plantilla
  const { rows: plantillaRows } = await pool.query(
    `SELECT id, nombre, asunto, html
       FROM core.email_templates
      WHERE codigo = $1`,
    [templateCode]
  );

  if (!plantillaRows.length)
    throw new Error(`No existe plantilla con código ${templateCode}`);

  const template = plantillaRows[0];

  // 2. Obtener usuarios
  const { rows: users } = await pool.query(
    `SELECT id, name AS nombre, email
       FROM core.users
      WHERE id = ANY($1::int[])`,
    [userIds]
  );

  // 3. Envío individual con reintentos
  for (const user of users) {
    const finalData = {
      ...data,
      NOMBRE_COMPLETO: user.nombre
    };

    const subject = renderTemplate(template.asunto, finalData);
    const finalHtml = renderTemplate(template.html, finalData);

    let attempt = 0;
    let success = false;
    let lastError = null;

    while (attempt < BULK_RETRY_LIMIT && !success) {
      attempt++;

      try {
        await transporter.sendMail({
          from: `"MOVILCO – Grupo Empresarial" <noreply@devmauricioy.com>`,
          to: user.email,
          subject,
          html: finalHtml
        });

        await logEmail({
          userId: user.id,
          templateId: template.id,
          to: user.email,
          subject,
          html: finalHtml,
          estado: "SENT",
          payload: finalData
        });

        results.push({
          userId: user.id,
          email: user.email,
          attempt,
          status: "SENT",
          asunto: subject,
        });

        success = true;

      } catch (err) {
        lastError = err;

        if (attempt >= BULK_RETRY_LIMIT) {
          await logEmail({
            userId: user.id,
            templateId: template.id,
            to: user.email,
            subject,
            html: finalHtml,
            estado: "FAIL",
            errorMsg: err.message,
            payload: finalData
          });

          results.push({
            userId: user.id,
            email: user.email,
            attempt,
            status: "FAIL",
            asunto: subject,
            error: err.message
          });
        }
      }
    }

    await new Promise(resolve => setTimeout(resolve, BULK_THROTTLE_MS));
  }

  return {
    ok: true,
    message: `Proceso masivo finalizado: ${results.filter(r => r.status === "SENT").length} correos enviados.`,
    template: {
      code: templateCode,
      nombre: template.nombre,
      asunto_muestra: renderTemplate(template.asunto, data)
    },
    total: results.length,
    enviados: results.filter(r => r.status === "SENT").length,
    fallidos: results.filter(r => r.status === "FAIL").length,
    detalles: results
  };
}

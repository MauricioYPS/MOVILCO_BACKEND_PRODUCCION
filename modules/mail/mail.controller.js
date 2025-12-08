// modules/mail/mail.controller.js

import pool from "../../config/database.js";
import { renderTemplate } from "./render-template.js";
import { getTemplateByCode, sendTemplateEmail, sendBulkEmails } from "./mail.service.js";

/**
 * PREVIEW HTML
 * ----------------------------
 * POST /api/mail/preview
 * Permite ver cómo quedará el correo antes de enviarlo.
 * No envía nada, solo renderiza la plantilla con las variables dadas.
 */
export async function previewTemplate(req, res) {
  try {
    const { templateCode, data = {} } = req.body;

    if (!templateCode) {
      return res.status(400).json({ ok: false, error: "Falta templateCode" });
    }

    // Obtener plantilla desde BD
    const template = await getTemplateByCode(templateCode);

    // Renderizar con variables
    const asuntoFinal = renderTemplate(template.asunto, data);
    const htmlFinal = renderTemplate(template.html, data);

    return res.json({
      ok: true,
      templateCode,
      asunto: asuntoFinal,
      html: htmlFinal,
    });

  } catch (err) {
    console.error("[Mail Preview] Error:", err);
    return res.status(500).json({
      ok: false,
      error: "Error generando preview",
      detail: err.message,
    });
  }
}



/**
 * SEND INDIVIDUAL TEMPLATE
 * ----------------------------
 * POST /api/mail/send-template
 * Envía un correo usando una plantilla existente.
 */
export async function sendTemplate(req, res) {
  try {
    const { templateCode, to, userId, data = {}, period = null } = req.body;

    if (!templateCode || !to || !userId) {
      return res.status(400).json({
        ok: false,
        error: "Faltan parámetros obligatorios: templateCode, to, userId",
      });
    }

    const result = await sendTemplateEmail({
      templateCode,
      to,
      data,
      userId,
      period,
    });

    return res.json(result);

  } catch (err) {
    console.error("[Mail Send] Error:", err);

    return res.status(500).json({
      ok: false,
      error: "Error enviando correo",
      detail: err.message,
    });
  }
}



/**
 * SEND MASSIVE EMAILS
 * ----------------------------
 * POST /api/mail/send/bulk
 * Envía correos a una lista de usuarios.
 *
 * usersList = [
 *   { email, user_id, variables: {NOMBRE_COMPLETO:"...", MES:"..."} }
 * ]
 */

export async function sendBulk(req, res) {
  try {
    const { templateCode, userIds = [], data = {} } = req.body;

    // 1. Validaciones
    if (!templateCode) {
      return res.status(400).json({ ok: false, error: "Falta templateCode" });
    }

    if (!Array.isArray(userIds) || userIds.length === 0) {
      return res.status(400).json({
        ok: false,
        error: "userIds debe ser un array con al menos un usuario",
      });
    }

    // 2. Ejecutar proceso masivo avanzado
    const result = await sendBulkEmails({ templateCode, userIds, data });

    // 3. Respuesta completa
    return res.json({
      ok: true,
      message: "Proceso masivo finalizado",
      total: result.total,
      enviados: result.enviados,
      fallidos: result.fallidos,
      detalles: result.detalles,
    });

  } catch (err) {
    console.error("[MAIL BULK ERROR]", err);

    return res.status(500).json({
      ok: false,
      error: "Error en envío masivo",
      detail: err.message,
    });
  }
}

import { sendEmailTemplate, sendEmailBatch } from "../../services/emailSenderService.js";
import { getEmailTemplateByCode } from "../../services/emailTemplateService.js";

export async function sendTemplate(req, res) {
  try {
    const codigo = req.params.codigo;
    const { to, data } = req.body;

    const result = await sendEmailTemplate({ codigo, to, data });

    return res.json(result);
  } catch (err) {
    return res.status(500).json({ ok: false, error: err.message });
  }
}

export async function sendTemplateBatch(req, res) {
  try {
    const codigo = req.params.codigo;
    const { recipients, data } = req.body;

    const result = await sendEmailBatch({ codigo, recipients, data });

    return res.json({ ok: true, result });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err.message });
  }
}

export async function getTemplate(req, res) {
  try {
    const codigo = req.params.codigo;
    const template = await getEmailTemplateByCode(codigo);

    if (!template) {
      return res.status(404).json({ ok: false, error: "Plantilla no encontrada" });
    }

    return res.json({ ok: true, template });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err.message });
  }
}

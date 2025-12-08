// modules/mail/mail.router.js

import { Router } from "express";
import { previewTemplate,sendTemplate,sendBulk} from "../modules/mail/mail.controller.js";
import { sendBulkTemplate } from "../modules/mail/mail.service.js";
const router = Router();

/**
 *  @route POST /api/mail/preview
 *  @desc Previsualizar una plantilla con variables
 */
router.post("/preview", previewTemplate);

/**
 *  @route POST /api/mail/send-template
 *  @desc Enviar un correo individual usando una plantilla
 */
router.post("/send-template", sendTemplate);

/**
 *  @route POST /api/mail/send/bulk
 *  @desc Enviar correos masivos usando una plantilla
 */
router.post("/send/bulk", sendBulk);

export default router;

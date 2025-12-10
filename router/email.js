import { Router } from "express";
import {
  sendTemplate,
  sendTemplateBatch,
  getTemplate
} from "../controllers/email/emailController.js";

const router = Router();

// Enviar 1 correo con plantilla
router.post("/send-template/:codigo", sendTemplate);

// Enviar a muchos destinatarios
router.post("/send-batch/:codigo", sendTemplateBatch);

// Obtener datos de una plantilla
router.get("/template/:codigo", getTemplate);

export default router;

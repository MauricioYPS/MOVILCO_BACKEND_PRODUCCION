import { Router } from "express";
import { exportSIAPPController } from "../controllers/export/exportSIAPP.controller.js";
import { exportNominaController } from "../controllers/export/exportNomina.controller.js";


const router = Router();
router.get('/siapp', exportSIAPPController);
router.get('/nomina', exportNominaController);

export default router
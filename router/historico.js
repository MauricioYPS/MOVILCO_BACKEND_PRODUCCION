import { Router } from "express";
import {
  listHistoricoSiapp,
  getHistoricoSiapp,
} from "../controllers/historico/historico.controller.js";
import {
  exportHistoricoSiappExcel,
  exportHistoricoPresupuesto
} from "../controllers/historico/historico.excel.controller.js";
const router = Router();

// SIAPP FULL
router.get("/siapp", listHistoricoSiapp);
router.get("/siapp/:periodo", getHistoricoSiapp);

// PRESUPUESTO JERARQUÍA
// router.get("/presupuesto", listHistoricoPresupuesto);
// router.get("/presupuesto/:periodo", getHistoricoPresupuesto);

router.get("/siapp/:periodo_backup/excel", exportHistoricoSiappExcel);
router.get("/presupuesto/:periodo/excel", exportHistoricoPresupuesto);


export default router;

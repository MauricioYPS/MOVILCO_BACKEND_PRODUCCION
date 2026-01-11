// router/promote.js
import { Router } from 'express'
import { promoteEstructura } from '../controllers/promote/estructura.js'
import { promoteNomina } from '../controllers/promote/nomina.js'
import { promotePresupuesto } from "../controllers/promote/presupuesto.js";
import { promoteSiapp } from '../controllers/promote/siapp.js'
// import { promoteNovedades } from '../controllers/promote/novedades.js'
import { promotePresupuestoUsuariosController } from '../controllers/promote/presupuesto_usuarios.js'
import { promoteSiappFULL } from '../controllers/promote/siapp_full.js'
import { normalizeJerarquiaController } from "../controllers/promote/normalize_jerarquia.js";
import { promotePresupuestoJerarquiaController } from "../controllers/promote/presupuesto_jerarquia.js";
import { promoteSiappBatch } from "../controllers/promote/siapp.batch.js";
import { promoteNovedadesController } from '../controllers/promote/novedades.controller.js';

const router = Router()
router.post('/estructura', promoteEstructura)
router.post('/nomina', promoteNomina)
router.post('/presupuesto', promotePresupuesto)
router.post('/siapp', promoteSiapp)
router.post('/presupuesto_usuarios', promotePresupuestoUsuariosController)
// router.post('/novedades', promoteNovedades)
router.post('/novedades', promoteNovedadesController) //Novedades mediante excell
router.post('/siapp/full', promoteSiappFULL)
router.post('/siapp_full', promoteSiappFULL)
router.post("/normalize-jerarquia", normalizeJerarquiaController);
router.post("/presupuesto-jerarquia", promotePresupuestoJerarquiaController);
router.post("/siapp/batch", promoteSiappBatch);

export default router

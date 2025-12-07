import { Router } from 'express'
import { getUnitKpi } from '../controllers/kpi/unit.js'
import { getLevelKpi } from '../controllers/kpi/level.js'
import { kpiCalculateController } from '../controllers/kpi/calculate.js';
import { saveKpiController } from '../controllers/kpi/save.js';
import { getKpiController } from "../controllers/kpi/get.js";
import { getUnknownAdvisorsController } from "../controllers/kpi/unknown.js";



const router = Router()

router.get('/unit/:unit_id', getUnitKpi)          // ?period=YYYY-MM
router.get('/level/:unit_type', getLevelKpi)      // GERENCIA|DIRECCION|COORDINACION + ?period=YYYY-MM
router.get('/calculate', kpiCalculateController);
router.post('/save', saveKpiController);
router.get('/get', getKpiController);
router.get('/unknown', getUnknownAdvisorsController);

export default router

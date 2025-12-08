import { Router } from 'express'
import { getUnitKpi } from '../controllers/kpi/unit.js'
import { getLevelKpi } from '../controllers/kpi/level.js'
import { kpiCalculateController } from '../controllers/kpi/calculate.js';
import { saveKpiController } from '../controllers/kpi/save.js';
import { getKpiController } from "../controllers/kpi/get.js";
import { getUnknownAdvisorsController } from "../controllers/kpi/unknown.js";
import {postDiasManualController,getDiasManualController,deleteDiasManualController
} from "../controllers/kpi/dias-manual.js";
import {postNovedadController,getNovedadesController,deleteNovedadController} from "../controllers/kpi/novedades.js";


const router = Router()

router.get('/unit/:unit_id', getUnitKpi)   
router.get('/level/:unit_type', getLevelKpi)      
router.get('/calculate', kpiCalculateController);
router.post('/save', saveKpiController);
router.get('/get', getKpiController);
router.get('/unknown', getUnknownAdvisorsController);
router.post("/manualdays", postDiasManualController);
router.get("/manualdays", getDiasManualController);
router.post("/novedades", postNovedadController);
router.get("/novedades", getNovedadesController);
router.delete("/novedades/:id", deleteNovedadController);

router.delete("/manualdays/:id", deleteDiasManualController);
export default router

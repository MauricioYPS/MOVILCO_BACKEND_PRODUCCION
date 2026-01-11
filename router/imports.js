// router/imports.js
import { Router } from 'express'
import { upload } from '../config/upload.js'        // Upload a disco (para procesos antiguos)
import multer from 'multer'
import { importDataset } from '../controllers/imports/upload.js'
import { importPresupuestoJerarquiaController } from "../controllers/imports/presupuesto_jerarquia.js";
import { importSiappFullController } from '../controllers/imports/siapp_full.js'
import { importPresupuesto } from '../controllers/imports/presupuesto.js';
import { importNovedadesController } from '../controllers/imports/novedades.controller.js';

const router = Router()
const uploadMemory = multer({ storage: multer.memoryStorage() })

router.post("/presupuesto-jerarquia",uploadMemory.single("file"),importPresupuestoJerarquiaController
);

router.post('/presupuesto', uploadMemory.single('file'), importPresupuesto)
router.post('/novedades', uploadMemory.single('file'), importNovedadesController)

// Nuevo: multer en memoria SOLO para siapp_full
router.post('/siapp_full', uploadMemory.single('file'), importSiappFullController)

// dataset ∈ {estructura, presupuesto, siapp, nomina}
router.post('/:dataset', upload.single('file'), importDataset)

// Nuevo endpoint usando upload en memoria

export default router

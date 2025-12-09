import { Router } from "express";
import { getRegionalDirections } from "../controllers/regional/regional.controller.js";
import {getAllNovedadesGerenciales} from "../controllers/regional/novedades.controller.js"

const router = Router();

router.get("/directions", getRegionalDirections);
router.get("/novedades", getAllNovedadesGerenciales);

export default router;

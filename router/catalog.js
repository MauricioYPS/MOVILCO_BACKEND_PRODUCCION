// routes/catalog.routes.js
import { Router } from "express";
import { regions, districts, districtsClaro, coordinators } from "../controllers/catalog/read.js";

const router = Router();

router.get("/regions", regions);
router.get("/districts", districts);
router.get("/districts-claro", districtsClaro);
router.get("/coordinators", coordinators);

export default router;

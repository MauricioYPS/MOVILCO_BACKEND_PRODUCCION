// router/sync.js
import { Router } from "express";
import { syncPresupuestoJerarquiaController } from "../controllers/sync/presupuesto_jerarquia.js";

const router = Router();

router.post("/presupuesto-jerarquia", syncPresupuestoJerarquiaController);

export default router;

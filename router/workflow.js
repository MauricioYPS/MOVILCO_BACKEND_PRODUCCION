import { Router } from "express";
import { approveAndMoveToCoordinator, exportCoordinatorSale } from "../controllers/workflow/workflow.controller.js";
import { exportAllCoordinatorSales } from "../controllers/workflow/export-month.controller.js";

const router = Router();

/** Aprobar RAW y mover a tabla del coordinador */
router.put("/sales/workflow/approve/:id", approveAndMoveToCoordinator);

/** Exportar venta del coordinador al SIAPP final */
router.put("/sales/workflow/export/:id", exportCoordinatorSale);

/** Exportar todas las ventas del coordinador para un mes */
router.put("/sales/workflow/export-month", exportAllCoordinatorSales);

export default router;

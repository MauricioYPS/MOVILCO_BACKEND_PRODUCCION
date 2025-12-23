import { Router } from "express";
import {
  postCoordinatorSale,
  getCoordinatorSalesController,
  putCoordinatorSale,
  markSaleExportController
} from "../controllers/coordinator/coordinatorSales.controller.js";

const router = Router();

/* ------------------------- RUTAS COORDINADOR ------------------------- */

/** Crear venta revisada/aprobada por coordinador */
router.post("/sales/coordinator", postCoordinatorSale);

/** Obtener ventas aprobadas por coordinador */
router.get("/sales/coordinator", getCoordinatorSalesController);

/** Actualizar venta corregida por coordinador */
router.put("/sales/coordinator/:id", putCoordinatorSale);

/** Marcar venta lista para exportar al SIAPP generado */
router.put("/sales/coordinator/ready/:id", markSaleExportController);

export default router;
import { Router } from "express";
import {
  postAdvisorRawSale,
  getAdvisorRawSalesController,
  putAdvisorRawSale,
  deleteAdvisorRawSaleController,
  getPendingAdvisorSales,
  setAdvisorRawSaleStatusController
} from "../controllers/advisor/advisorSalesRaw.controller.js";

const router = Router();

/* ----------------------------- RUTAS ASESOR ----------------------------- */

/** Crear venta del asesor */
router.post("/sales/raw", postAdvisorRawSale);

/** Obtener ventas del asesor (historial por mes) */
router.get("/sales/raw", getAdvisorRawSalesController);

/** Actualizar venta del asesor */
router.put("/sales/raw/:id", putAdvisorRawSale);

/** Eliminar venta del asesor */
router.delete("/sales/raw/:id", deleteAdvisorRawSaleController);

/* -------------------------- RUTAS COORDINADOR --------------------------- */

/** Obtener ventas pendientes para revisión */
router.get("/sales/raw/pending", getPendingAdvisorSales);

/** Cambiar estado de revisión (pendiente → aprobado/rechazado) */
router.put("/sales/raw/status/:id", setAdvisorRawSaleStatusController);

export default router;
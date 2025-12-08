import { Router } from "express";
import {
  postGeneratedSale,
  getGeneratedSalesController
} from "../controllers/siapp/generatedSales.controller.js";

const router = Router();

/* -------------------------- RUTAS SIAPP FINAL -------------------------- */

/** Crear venta final en SIAPP generado */
router.post("/sales/generated", postGeneratedSale);

/** Obtener ventas del SIAPP generado por mes */
router.get("/sales/generated", getGeneratedSalesController);

export default router;


// routers/siapp.js
import { Router } from "express";
import { getSiappPeriods } from "../controllers/siapp/periods.js";
import { monthlyNominaPreview } from "../controllers/siapp/monthlyNomina.js";
import { getMonthlyAdvisors } from "../controllers/siapp/monthlyAdvisors.js";
import { getMonthlySales } from "../controllers/siapp/monthly.sales.controller.js";
const router = Router();

router.get("/periods", getSiappPeriods);
router.get("/monthly/nomina", monthlyNominaPreview);
router.get("/monthly/advisors", getMonthlyAdvisors);
router.get("/monthly/sales", getMonthlySales);
export default router;

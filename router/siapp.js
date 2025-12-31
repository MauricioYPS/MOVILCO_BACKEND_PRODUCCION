// routers/siapp.js
import { Router } from "express";
// import { getSiappPeriods } from "../controllers/siapp/periods.js";
import { getSiappPeriods } from "../controllers/siapp/periods.controller.js";
import { monthlyNominaPreview } from "../controllers/siapp/monthlyNomina.js";
// import { getMonthlyAdvisors } from "../controllers/siapp/monthlyAdvisors.js";
import {getMonthlyAdvisorsController} from "../controllers/siapp/monthly.advisors.controller.js";
import { getMonthlySalesSummaryController } from "../controllers/siapp/monthly.sales.summary.controller.js";
import { getMonthlySales } from "../controllers/siapp/monthly.sales.controller.js";
import { closeMonthly} from "../controllers/siapp/monthly.close.controller.js";
import { getMonthlyProgressSummaryController } from "../controllers/siapp/monthly.progress.summary.controller.js";
import {getMonthlyProgressDetailsController} from "../controllers/siapp/monthly.progress.details.controller.js";
const router = Router();

// router.get("/periods", getSiappPeriods);
router.get("/periods", getSiappPeriods);
router.get("/monthly/nomina", monthlyNominaPreview);
// router.get("/monthly/advisors", getMonthlyAdvisors);
router.get("/monthly/advisors", getMonthlyAdvisorsController);
router.get("/monthly/sales", getMonthlySales);
router.get("/monthly/sales/summary", getMonthlySalesSummaryController);
router.post("/monthly/close", closeMonthly);
router.get("/monthly/progress/summary", getMonthlyProgressSummaryController);
router.get("/monthly/progress/details", getMonthlyProgressDetailsController);

export default router;

// routes/budgets.js
import { Router } from "express";
import { authRequired } from "../middlewares/authRequired.js";
import { allowRoles } from "../middlewares/allowRoles.js";

import {
  getBudgetsTreeController,
  getBudgetsController,
  getMissingBudgetsController,
  putBudgetsBatchController,
  putBudgetByIdController,
  postCopyBudgetsController
} from "../controllers/budgets/budgets.controller.js";

const router = Router();

// Protección JWT (valida activo en BD)
// router.use(authRequired);

// Lecturas: RRHH/ADMIN + negocio (si lo deseas)
// router.get(
//   "/tree",
//   allowRoles("ADMIN", "RECURSOS_HUMANOS", "GERENCIA", "DIRECCION", "COORDINACION"),
//   getBudgetsTreeController
// );

// router.get(
//   "/",
//   allowRoles("ADMIN", "RECURSOS_HUMANOS", "GERENCIA", "DIRECCION", "COORDINACION"),
//   getBudgetsController
// );

// router.get(
//   "/missing",
//   allowRoles("ADMIN", "RECURSOS_HUMANOS"),
//   getMissingBudgetsController
// );

// // Escrituras: solo RRHH/ADMIN
// router.put(
//   "/batch",
//   allowRoles("ADMIN", "RECURSOS_HUMANOS"),
//   putBudgetsBatchController
// );

// router.put(
//   "/:id",
//   allowRoles("ADMIN", "RECURSOS_HUMANOS"),
//   putBudgetByIdController
// );

// router.post(
//   "/copy",
//   allowRoles("ADMIN", "RECURSOS_HUMANOS"),
//   postCopyBudgetsController
// );
router.get("/tree", getBudgetsTreeController);
router.get("/", getBudgetsController);
router.get("/missing", getMissingBudgetsController);
router.put("/batch", putBudgetsBatchController);
router.put("/:id", putBudgetByIdController);
router.post("/copy", postCopyBudgetsController);

export default router;

import express from "express";

import { authRequired } from "../middlewares/authRequired.js"; // usa el que ya tienes
import { createNoveltyManualController } from "../controllers/novedades/create.manual.js";

import {
  listNoveltiesController,
  recentNoveltiesController,
  getNoveltyByIdController,
  updateNoveltyController,
  deleteNoveltyController
} from "../controllers/novedades/index.js";

const router = express.Router();

// Todas requieren auth
router.use(authRequired);

// CREATE
// POST /api/novedades/manual
router.post("/manual", createNoveltyManualController);

// READ (LIST)
// GET /api/novedades?date_from=&date_to=&q=&limit=&offset=
router.get("/", listNoveltiesController);

// GET /api/novedades/recent?days=3&limit=50
router.get("/recent", recentNoveltiesController);

// READ (DETAIL)
// GET /api/novedades/:id
router.get("/:id", getNoveltyByIdController);

// UPDATE
// PUT /api/novedades/:id
router.put("/:id", updateNoveltyController);

// DELETE
// DELETE /api/novedades/:id
router.delete("/:id", deleteNoveltyController);

export default router;

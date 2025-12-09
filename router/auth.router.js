import { Router } from "express";
import { register, login } from "../controllers/auth/auth.controller.js";
import { rateLimitLogin } from "../middlewares/rateLimitLogin.js";
import { logout } from "../controllers/auth/auth.controller.js";
const router = Router();

router.post("/register", register);
router.post("/login", rateLimitLogin, login);
router.post("/logout", logout);

export default router;

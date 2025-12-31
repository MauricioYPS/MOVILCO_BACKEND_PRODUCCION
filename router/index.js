import { authRequired} from "../middlewares/authRequired.js"
import { Router } from "express";
import unitsRouter from './units.js'
import usersRouter from './user.js'
import importsRouter from './imports.js'
import promoteRouter from './promote.js'
import stagingRouter from './staging.js'
import kpiRouter from './kpi.js'
import settingsRouter from './settings.js'
import reportsRouter from './reports.js'
import analyticsRouter from './analytics.js'
import advisorRouter from './advisor.js'
import coordinatorRouter from './coordinator.js'
import generateSalesRouter from './generateSales.js'
import workflowRouter from './workflow.js'
import exportRouter from './export.js'
import mailRouter from './mail.js'
import authRouter from './auth.router.js'
import regionalRouer from './regional.js'
import emailRouter from './email.js'
import historicoRouter from './historico.js'
import siappRouter from './siapp.js'
import synRouter from './sync.js'
import novedadesRouter from './novedades.js'
import catalogRouter from './catalog.js'

const router = Router()

router.use('/auth',authRouter)
// router.use(authRequired) // Protección de rutas siguientes
router.use('/org',unitsRouter)
router.use('/analytics',analyticsRouter)
router.use('/users',usersRouter)
router.use('/imports',importsRouter)
router.use('/promote',promoteRouter)
router.use('/staging',stagingRouter)
router.use('/kpi' ,kpiRouter)
router.use('/settings' ,settingsRouter)
router.use('/reports' ,reportsRouter)
router.use('/advisor' ,advisorRouter)
router.use('/coordinator' ,coordinatorRouter)
router.use('/generate-sales',generateSalesRouter)
router.use('/workflow' ,workflowRouter)
router.use('/export' ,exportRouter)
router.use('/mail' ,mailRouter)
router.use('/regional' ,regionalRouer)
router.use('/email' ,emailRouter)
router.use('/historico' ,historicoRouter)
router.use('/siapp' ,siappRouter)
router.use('/sync' ,synRouter)
router.use('/novedades', novedadesRouter)
router.use('/catalog', catalogRouter)

export default router
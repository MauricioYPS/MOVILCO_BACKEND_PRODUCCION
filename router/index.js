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


const router = Router()

router.use('/org',unitsRouter)
router.use('/analytics', analyticsRouter)
router.use('/users', usersRouter)
router.use('/imports', importsRouter)
router.use('/promote', promoteRouter)
router.use('/staging', stagingRouter)
router.use('/kpi', kpiRouter)
router.use('/settings', settingsRouter)
router.use('/reports', reportsRouter)
export default router
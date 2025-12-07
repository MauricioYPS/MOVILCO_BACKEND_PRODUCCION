import express from 'express'
import { progressSummary, progressByUser } from '../controllers/analytics/progress.js'

const router = express.Router()

router.get('/progress/summary', progressSummary)
router.get('/progress/rows', progressByUser)

export default router
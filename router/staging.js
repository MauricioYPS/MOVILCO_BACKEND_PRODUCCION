import { Router } from 'express'
import { listStaging } from '../controllers/staging/list.js'

const router = Router()
router.get('/:dataset', listStaging)

export default router

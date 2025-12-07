import { Router } from 'express'
import { getSettings } from '../controllers/settings/get.js'

const router = Router()
router.get('/', getSettings)

export default router

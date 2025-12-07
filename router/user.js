import { Router } from 'express'
import { list, getOne, getAdvisorsByCoordinator } from '../controllers/users/read.js'
import { create } from '../controllers/users/create.js'
import { update } from '../controllers/users/update.js'
import { remove } from '../controllers/users/delete.js'

const router = Router()

router.get('/', list)
router.get('/:id', getOne)
router.get('/coordinator/:id', getAdvisorsByCoordinator)

router.post('/', create)
router.put('/:id', update)
router.delete('/:id', remove)

export default router

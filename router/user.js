import { Router } from 'express'
import { list, getOne} from '../controllers/users/read.js'
import { create } from '../controllers/users/create.js'
import { update } from '../controllers/users/update.js'
import { remove } from '../controllers/users/delete.js'
import { getAsesoresByCoordinador, getCoordinadoresByDireccion, getDireccionesByGerencia} from '../controllers/users/hierarchy.controller.js'
import {getCoordinadoresByDireccionV2} from '../controllers/users/hierarchy.controller.V2.js'
import { authRequired } from '../middlewares/authRequired.js'
import { validateCoordinatorAccess } from '../middlewares/validateCoordinatorAccess.js'
import { validateDirectionAccess } from '../middlewares/validateDirectionAccess.js'
import { validateGerenciaAccess } from '../middlewares/validateGerenciaAccess.js'
import { getUserFullProfile } from '../controllers/users/user.profile.controller.js'
import { getUsersByDirector, getCoordinadoresByDirector } from '../controllers/users/read.js'
const router = Router()

router.get('/', list)
router.get('/by-coordinator/:id' , getAsesoresByCoordinador)
router.get('/by-direction/:id', getCoordinadoresByDireccion)
router.get('/by-management/:gerencia_id', getDireccionesByGerencia)
router.get('/by-direction-v2/:direction_id' ,getCoordinadoresByDireccionV2)
router.get('/profile/:id',getUserFullProfile)
router.get('/by-director/:id',getUsersByDirector)
router.get('/director/:id/coordinadores',getCoordinadoresByDirector)
router.post('/', create)
router.put('/:id', update)
router.delete('/:id', remove)

router.get('/:id', getOne)


export default router
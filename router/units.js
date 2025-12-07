import { Router } from 'express'
import { list, getOne, tree } from '../controllers/units/read.js'
import { create } from '../controllers/units/create.js'
import { update } from '../controllers/units/update.js'
import { remove } from '../controllers/units/delete.js'
import { root } from '../controllers/units/read.js'
import { getJerarquiaTreeController } from "../controllers/units/get_tree.js";

const router = Router()

router.get('/units', list)
router.get('/units/tree', tree)
router.get('/units/:id', getOne)
router.get('/units-root', root)
router.get("/units/jerarquia", getJerarquiaTreeController);

router.post('/units', create)
router.put('/units/:id', update)
router.delete('/units/:id', remove)

export default router

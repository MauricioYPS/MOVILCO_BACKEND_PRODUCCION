import { Router } from 'express'
import { getPayrollReport,payrollDetail, getBasicPayrollForCoordinator  } from '../controllers/reports/payroll.js'
import { consolidated } from '../controllers/reports/consolidated.js'
import { exportNominaController } from '../controllers/payroll/export_nomina.js';

const router = Router()

// GET /api/reports/payroll?period=YYYY-MM&format=xlsx|csv&unit_id=123
router.get('/payroll', getPayrollReport)
router.get('/payroll/detail', payrollDetail)
router.get('/consolidated', consolidated)
router.get('/payroll/coordinator/basic', getBasicPayrollForCoordinator);
router.get('/payroll/nomina', exportNominaController);



export default router

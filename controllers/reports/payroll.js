// controllers/reports/payroll.js
import {
  getPayrollDetailRowFULL,
  fetchBasicForCoordinatorFull,
  generatePayrollReportFULL
} from '../../services/report.payroll.service.js'

// ===============================
//  GENERAR ARCHIVO DE NÓMINA
// ===============================
export async function getPayrollReport(req, res) {
  try {
    const { period, format = 'xlsx', unit_id } = req.query;

    if (!period) {
      return res.status(400).json({ error: "Missing period (YYYY-MM)" });
    }

    const rpt = await generatePayrollReportFULL({
      period,
      unit_id: unit_id ? Number(unit_id) : null,
      format: String(format).toLowerCase()
    });

    res.setHeader('Content-Type', rpt.mime);
    res.setHeader('Content-Disposition', `attachment; filename="${rpt.filename}"`);
    res.send(rpt.buffer);

  } catch (e) {
    console.error('[REPORT payroll]', e);
    res.status(400).json({
      error: 'No se pudo generar el reporte',
      detail: e.message
    });
  }
}

// ===============================
//  DETALLE INDIVIDUAL DE ASESOR
// ===============================
export async function payrollDetail(req, res) {
  try {
    const { period, user_id } = req.query;

    if (!period || !user_id) {
      return res.status(400).json({ error: "Missing period or user_id" });
    }

    const row = await getPayrollDetailRowFULL({ period, user_id });

    if (!row) {
      return res.status(404).json({
        error: 'No se encontro informacion para ese usuario y periodo'
      });
    }

    return res.json(row);

  } catch (e) {
    console.error('[REPORT payroll detail]', e);
    return res.status(400).json({ error: e.message });
  }
}

// ===============================
//  RESUMEN PARA COORDINADOR
// ===============================
export async function getBasicPayrollForCoordinator(req, res) {
  try {
    const { period, coordinator_id } = req.query;

    if (!period) {
      return res.status(400).json({ error: "Missing period (YYYY-MM)" });
    }

    if (!coordinator_id) {
      return res.status(400).json({ error: "Missing coordinator_id" });
    }

    const match = period.match(/^(\d{4})-(\d{2})$/);
    if (!match) {
      return res.status(400).json({ error: "Invalid period format" });
    }

    const year = Number(match[1]);
    const month = Number(match[2]);

    const data = await fetchBasicForCoordinatorFull({
      period_year: year,
      period_month: month,
      coordinator_id: Number(coordinator_id)
    });

    return res.json(data);

  } catch (e) {
    console.error("[getBasicPayrollForCoordinator]", e);
    return res.status(500).json({
      error: "Internal error",
      detail: e.message
    });
  }
}

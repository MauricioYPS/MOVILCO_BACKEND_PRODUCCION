import { exportNominaExcel } from '../../services/payroll.nomina.export.service.js';

export async function exportNominaController(req, res) {
  try {
    const { period } = req.query;

    const file = await exportNominaExcel({ period });

    res.setHeader("Content-Type", file.mime);
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${file.filename}"`
    );

    return res.send(file.buffer);

  } catch (e) {
    console.error("[EXPORT NOMINA ERROR]", e);
    return res.status(500).json({
      error: "No se pudo generar el archivo nómina",
      detail: e.message
    });
  }
}

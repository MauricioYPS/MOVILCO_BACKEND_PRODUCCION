// controllers/kpi/unknown.js
import { getUnknownAdvisors } from "../../services/kpi.unknown.service.js";

export async function getUnknownAdvisorsController(req, res) {
  try {
    const { period, detalle, fecha, documento } = req.query;

    /**********************************************************
     * 1. Validación del parámetro ?period=YYYY-MM
     **********************************************************/
    if (!period) {
      return res.status(400).json({
        ok: false,
        error: "Falta ?period=YYYY-MM"
      });
    }

    const match = String(period).match(/^(\d{4})-(\d{2})$/);
    if (!match) {
      return res.status(400).json({
        ok: false,
        error: "El formato de ?period debe ser YYYY-MM"
      });
    }

    /**********************************************************
     * 2. Convertir ?detalle a booleano
     **********************************************************/
    const showDetail = detalle === "true";

    /**********************************************************
     * 3. Validación del filtro de fecha real
     **********************************************************/
    let fechaFiltro = null;

    if (fecha) {
      const m = String(fecha).match(/^(\d{4})-(\d{2})$/);
      if (!m) {
        return res.status(400).json({
          ok: false,
          error: "El parámetro ?fecha debe tener formato YYYY-MM"
        });
      }
      fechaFiltro = fecha;
    }

    /**********************************************************
     * 4. Documento (opcional)
     **********************************************************/
    const documentoFiltro = documento ? String(documento).trim() : null;

    /**********************************************************
     * 5. LOGS
     **********************************************************/
    console.log("\n========================================");
    console.log("[KPI UNKNOWN] Nueva solicitud");
    console.log(`Periodo: ${period}`);
    console.log(`Detalle: ${showDetail}`);
    console.log(`Filtro fecha SIAPP: ${fechaFiltro}`);
    console.log(`Documento: ${documentoFiltro}`);
    console.log("========================================\n");

    /**********************************************************
     * 6. Ejecutar servicio principal
     **********************************************************/
    const result = await getUnknownAdvisors(
      period,
      showDetail,
      fechaFiltro,
      documentoFiltro
    );

    /**********************************************************
     * 7. Respuesta al cliente
     **********************************************************/
    return res.json({
      ok: true,
      ...result
    });

  } catch (e) {
    console.error("\n🔥 [KPI UNKNOWN Controller] ERROR:");
    console.error(e);
    console.error("========================================\n");

    return res.status(500).json({
      ok: false,
      error: "No se pudo obtener asesores desconocidos",
      detail: e.message
    });
  }
}

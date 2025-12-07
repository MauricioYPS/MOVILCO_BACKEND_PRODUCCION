// controllers/promote/presupuesto_jerarquia.js
import { promotePresupuestoJerarquia } from "../../services/promote.presupuesto_jerarquia.service.js";

/**
 * Promueve los datos ya importados en staging.presupuesto_jerarquia
 * hacia:
 *   - core.org_units  (GERENCIA / DIRECCION / COORDINACION / DISTRITO)
 *   - core.users      (gerentes, directores, coordinadores, asesores)
 *
 * No recibe body, solo ejecuta la lógica de promote.
 */
export async function promotePresupuestoJerarquiaController(req, res) {
  try {
    console.log("[PROMOTE PJ] Iniciando promote de Presupuesto Jerarquía...");

    const result = await promotePresupuestoJerarquia();

    console.log("[PROMOTE PJ] Promote completado.");

    return res.json({
      ok: true,
      message: "Promoción de Presupuesto Jerarquía ejecutada correctamente",
      ...result
    });

  } catch (e) {
    console.error("[PROMOTE PJ] Error en promote Presupuesto Jerarquía:", e);

    return res.status(500).json({
      ok: false,
      error: "No se pudo promover Presupuesto Jerarquía a core.org_units / core.users",
      detail: e.message
    });
  }
}

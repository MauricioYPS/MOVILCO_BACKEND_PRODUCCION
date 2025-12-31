// controllers/promote/presupuesto_jerarquia.js
import { promotePresupuestoJerarquia } from "../../services/promote.presupuesto_jerarquia.service.js";

export async function promotePresupuestoJerarquiaController(req, res) {
  try {
    const { backup } = req.query;

    const do_backup =
      backup === "1" || String(backup).toLowerCase() === "true" || backup === true;

    console.log("[PROMOTE PJ] Iniciando promote Presupuesto Jerarquía...", { do_backup });

    const result = await promotePresupuestoJerarquia({ do_backup });

    return res.json({
      ok: true,
      message: "Promote Presupuesto Jerarquía ejecutado correctamente",
      ...result
    });
  } catch (e) {
    console.error("[PROMOTE PJ] Error:", e);
    return res.status(500).json({
      ok: false,
      error: "No se pudo promover Presupuesto Jerarquía",
      detail: e.message
    });
  }
}

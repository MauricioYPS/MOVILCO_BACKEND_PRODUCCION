// controllers/sync/presupuesto_jerarquia.js
import { syncPresupuestoJerarquiaToUsers } from "../../services/sync.presupuesto_jerarquia_to_users.service.js";

export async function syncPresupuestoJerarquiaController(req, res) {
  try {
    const { period, reset_all_users } = req.query;

    const reset =
      reset_all_users === "1" ||
      reset_all_users === "true" ||
      reset_all_users === true;

    console.log("[SYNC PJ->USERS] Iniciando sync jerarquía...", {
      period: period || null,
      reset_all_users: reset
    });

    const result = await syncPresupuestoJerarquiaToUsers({
      period,
      reset_all_users: reset
    });

    return res.json({
      ok: true,
      message: "Sync Presupuesto Jerarquía -> Users/OrgUnits ejecutado correctamente",
      ...result
    });
  } catch (e) {
    console.error("[SYNC PJ->USERS] Error:", e);
    return res.status(500).json({
      ok: false,
      error: "No se pudo sincronizar Presupuesto Jerarquía hacia core.users/core.org_units",
      detail: e.message
    });
  }
}

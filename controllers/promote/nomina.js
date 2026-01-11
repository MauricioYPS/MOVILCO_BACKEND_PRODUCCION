// controllers/promote/nomina.js
import { promoteNominaFromStaging } from "../../services/promote.nomina.service.js";

export async function promoteNomina(req, res) {
  try {
    const result = await promoteNominaFromStaging();

    return res.json({
      ok: true,
      ...result,
    });
  } catch (e) {
    console.error("[PROMOTE nomina]", e);
    return res.status(500).json({
      ok: false,
      error: "No se pudo promover la nómina a core.users",
      detail: e.message,
    });
  }
}

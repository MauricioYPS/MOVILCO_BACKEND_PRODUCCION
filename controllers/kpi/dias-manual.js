import { 
  setDiasLaboradosManual, 
  getDiasLaboradosManual, 
  deleteDiasLaboradosManual 
} from "../../services/kpi.dias-manual.service.js";

export async function postDiasManualController(req, res) {
  try {
    const { user_id, year, month, dias } = req.body;

    if (!user_id || !year || !month || dias === undefined) {
      return res.status(400).json({
        ok: false,
        error: "Faltan campos obligatorios"
      });
    }

    const result = await setDiasLaboradosManual({ user_id, year, month, dias });

    res.json({ ok: true, data: result });

  } catch (e) {
    console.error("ERROR SET DIAS MANUAL:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
}

export async function getDiasManualController(req, res) {
  try {
    const { user_id, year, month } = req.query;

    const result = await getDiasLaboradosManual({ user_id, year, month });

    res.json({ ok: true, data: result });

  } catch (e) {
    console.error("ERROR GET DIAS MANUAL:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
}

export async function deleteDiasManualController(req, res) {
  try {
    const { id } = req.params;

    await deleteDiasLaboradosManual(id);

    res.json({ ok: true });

  } catch (e) {
    console.error("ERROR DELETE DIAS MANUAL:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
}

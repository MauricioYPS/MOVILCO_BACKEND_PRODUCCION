import {
  setDiasLaboradosManual,
  getDiasLaboradosManual,
  listDiasLaboradosManualByPeriod,
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
    const { user_id, year, month, include_user } = req.query;

    if (!year || !month) {
      return res.status(400).json({
        ok: false,
        error: "Debe enviar year y month"
      });
    }

    const yy = Number(year);
    const mm = Number(month);

    if (!Number.isFinite(yy) || !Number.isFinite(mm) || mm < 1 || mm > 12) {
      return res.status(400).json({
        ok: false,
        error: "year/month inválidos"
      });
    }

    // 1) Si viene user_id => comportamiento actual (1 registro o null)
    if (user_id) {
      const uid = Number(user_id);
      if (!Number.isFinite(uid)) {
        return res.status(400).json({ ok: false, error: "user_id inválido" });
      }

      const result = await getDiasLaboradosManual({ user_id: uid, year: yy, month: mm });
      return res.json({ ok: true, data: result }); // data: {} o null
    }

    // 2) Si NO viene user_id => nuevo comportamiento: listado por periodo
    const list = await listDiasLaboradosManualByPeriod({
      year: yy,
      month: mm,
      include_user: String(include_user).toLowerCase() === "true"
    });

    return res.json({ ok: true, data: list }); // data: []
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

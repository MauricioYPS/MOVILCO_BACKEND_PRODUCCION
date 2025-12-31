// controllers/catalog/read.js
import {
  listRegions,
  listDistricts,
  listDistrictsClaro,
  listCoordinators
} from "../../services/catalog.service.js";

export async function regions(req, res) {
  try {
    const source = String(req.query.source || "coordinators").toLowerCase();
    const data = await listRegions({ source });
    res.json({ ok: true, source, total: data.length, items: data });
  } catch (e) {
    console.error("[GET /catalog/regions] error:", e);
    res.status(500).json({ ok: false, error: "Error listando regionales" });
  }
}


export async function districts(req, res) {
  try {
    const source = String(req.query.source || "nomina").toLowerCase();
    const data = await listDistricts({ source });
    res.json({ ok: true, source, total: data.length, items: data });
  } catch (e) {
    console.error("[GET /catalog/districts] error:", e);
    res.status(500).json({ ok: false, error: "Error listando distritos" });
  }
}

export async function districtsClaro(req, res) {
  try {
    const source = String(req.query.source || "nomina").toLowerCase();
    const data = await listDistrictsClaro({ source });
    res.json({ ok: true, source, total: data.length, items: data });
  } catch (e) {
    console.error("[GET /catalog/districts-claro] error:", e);
    res.status(500).json({ ok: false, error: "Error listando distritos claros" });
  }
}

export async function coordinators(req, res) {
  try {
    const activeOnly = String(req.query.active_only || "true").toLowerCase() === "true";
    const data = await listCoordinators({ activeOnly });
    res.json({ ok: true, total: data.length, items: data });
  } catch (e) {
    console.error("[GET /catalog/coordinators] error:", e);
    res.status(500).json({ ok: false, error: "Error listando coordinadores" });
  }
}

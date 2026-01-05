// controllers/budgets/budgets.controller.js
import {
  getBudgetsTree,
  getBudgetsByCoordinator,
  getMissingBudgets,
  upsertBudgetsBatch,
  updateBudgetById,
  copyBudgetsFromPreviousMonth
} from "../../services/budgets.service.js";

// Helper defensivo: compatibilidad currency -> unit
function patchCurrencyToUnit(obj) {
  if (!obj || typeof obj !== "object") return obj;

  // Si llega unit, lo dejamos. Si no, y llega currency, lo mapeamos.
  if (obj.unit == null && obj.currency != null) {
    obj.unit = obj.currency;
  }
  return obj;
}

export async function getBudgetsTreeController(req, res) {
  try {
    const { period, scope } = req.query;
    const result = await getBudgetsTree({ period, scope });
    return res.json({ ok: true, ...result });
  } catch (e) {
    return res.status(400).json({
      ok: false,
      error: e?.message || "Error consultando árbol de presupuestos"
    });
  }
}

export async function getBudgetsController(req, res) {
  try {
    const { period, coordinator_id, scope } = req.query;
    const result = await getBudgetsByCoordinator({ period, coordinator_id, scope });
    return res.json({ ok: true, ...result });
  } catch (e) {
    return res.status(400).json({
      ok: false,
      error: e?.message || "Error consultando presupuestos"
    });
  }
}

export async function getMissingBudgetsController(req, res) {
  try {
    const { period, scope, include_inactive } = req.query;
    const result = await getMissingBudgets({
      period,
      scope,
      include_inactive: include_inactive === "1" || include_inactive === "true"
    });
    return res.json({ ok: true, ...result });
  } catch (e) {
    return res.status(400).json({
      ok: false,
      error: e?.message || "Error consultando faltantes"
    });
  }
}

export async function putBudgetsBatchController(req, res) {
  try {
    const body = req.body || {};
    const { period, scope = "MONTHLY", items } = body;

    // Compatibilidad: si el front manda currency en items, lo convertimos a unit
    const normalizedItems = Array.isArray(items)
      ? items.map((it) => patchCurrencyToUnit({ ...it }))
      : items;

    // En modo pruebas puede no existir req.user
    const actor_user_id = req.user?.id ?? null;

    const result = await upsertBudgetsBatch({
      period,
      scope,
      items: normalizedItems,
      actor_user_id
    });

    return res.json({ ok: true, ...result });
  } catch (e) {
    return res.status(400).json({
      ok: false,
      error: e?.message || "Error guardando batch"
    });
  }
}

export async function putBudgetByIdController(req, res) {
  try {
    const { id } = req.params;
    const actor_user_id = req.user?.id ?? null;

    const patch = patchCurrencyToUnit({ ...(req.body || {}) });

    const updated = await updateBudgetById({
      id,
      patch,
      actor_user_id
    });

    return res.json({ ok: true, data: updated });
  } catch (e) {
    return res.status(400).json({
      ok: false,
      error: e?.message || "Error actualizando presupuesto"
    });
  }
}

export async function postCopyBudgetsController(req, res) {
  try {
    const body = req.body || {};
    const { period, scope = "MONTHLY" } = body;
    const actor_user_id = req.user?.id ?? null;

    const result = await copyBudgetsFromPreviousMonth({ period, scope, actor_user_id });
    return res.json({ ok: true, ...result });
  } catch (e) {
    return res.status(400).json({
      ok: false,
      error: e?.message || "Error copiando presupuestos"
    });
  }
}

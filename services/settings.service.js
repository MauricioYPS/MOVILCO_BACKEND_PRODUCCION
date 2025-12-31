// services/settings.service.js
import pool from "../config/database.js";

let cache = null;
let cacheTs = 0;
const TTL_MS = 60_000;

function toNumber(v, fallback) {
  if (v === null || v === undefined) return fallback;
  const s = String(v).trim();
  if (!s) return fallback;
  const n = Number(s.replace(",", "."));
  return Number.isFinite(n) ? n : fallback;
}

function toBool(v, fallback) {
  if (v === null || v === undefined) return fallback;
  const s = String(v).trim().toLowerCase();
  if (s === "true" || s === "1" || s === "yes" || s === "y") return true;
  if (s === "false" || s === "0" || s === "no" || s === "n") return false;
  return fallback;
}

export function clearSettingsCache() {
  cache = null;
  cacheTs = 0;
}

/**
 * loadSettings()
 * - Lee core.settings (key/value)
 * - Devuelve objeto tipado y con defaults
 * - Cache por TTL; usa force=true para recargar
 */
export async function loadSettings(force = false) {
  const now = Date.now();
  if (!force && cache && now - cacheTs < TTL_MS) return cache;

  const { rows } = await pool.query(`SELECT key, value FROM core.settings`);
  const map = new Map(rows.map((r) => [r.key, r.value]));

  cache = {
    // 100% por defecto
    compliance_threshold_percent: toNumber(map.get("compliance_threshold_percent"), 100),

    // usado para reglas de días (si lo sigues usando)
    month_days_mode: toNumber(map.get("month_days_mode"), 30),

    gerencia_name: map.get("gerencia_name") || "Gerencia Comercial",

    // booleano robusto
    use_user_monthly_prorrateo_first: toBool(map.get("use_user_monthly_prorrateo_first"), true)
  };

  cacheTs = now;
  return cache;
}

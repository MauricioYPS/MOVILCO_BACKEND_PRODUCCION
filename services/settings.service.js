import pool from '../config/database.js'

let cache = null
let cacheTs = 0
const TTL_MS = 60_000

export async function loadSettings(force = false) {
  const now = Date.now()
  if (!force && cache && now - cacheTs < TTL_MS) return cache
  const { rows } = await pool.query(`SELECT key, value FROM core.settings`)
  const map = new Map(rows.map(r => [r.key, r.value]))
  cache = {
    compliance_threshold_percent: Number(map.get('compliance_threshold_percent') || '100'),
    month_days_mode: Number(map.get('month_days_mode') || '30'),
    gerencia_name: map.get('gerencia_name') || 'Gerencia Comercial',
    use_user_monthly_prorrateo_first: String(map.get('use_user_monthly_prorrateo_first') || 'true') === 'true'
  }
  cacheTs = now
  return cache
}

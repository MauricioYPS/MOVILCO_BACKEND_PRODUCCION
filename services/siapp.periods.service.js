// services/siapp.periods.service.js
import pool from "../config/database.js";

/**
 * Lista los periodos disponibles en siapp.full_sales.
 * Devuelve: [{ period_year, period_month, filas, period }]
 *
 * Filtros opcionales:
 *  - year: 2025
 *  - from: "YYYY-MM"
 *  - to:   "YYYY-MM"
 *
 * Nota: NO cambia el shape de salida para no dañar el front.
 */
export async function listSiappPeriods({ year = null, from = null, to = null } = {}) {
  const parseYear = (v) => {
    if (v === null || v === undefined || String(v).trim() === "") return null;
    const n = Number(v);
    if (!Number.isFinite(n)) return null;
    return Math.trunc(n);
  };

  const parsePeriodToYYYYMM = (p) => {
    if (!p) return null;
    const s = String(p).trim();
    const m = s.match(/^(\d{4})-(\d{1,2})$/);
    if (!m) return null;
    const y = Number(m[1]);
    const mo = Number(m[2]);
    if (!Number.isFinite(y) || !Number.isFinite(mo) || mo < 1 || mo > 12) return null;
    return y * 100 + mo; // YYYYMM numérico
  };

  const y = parseYear(year);
  const fromYYYYMM = parsePeriodToYYYYMM(from);
  const toYYYYMM = parsePeriodToYYYYMM(to);

  if ((from && fromYYYYMM === null) || (to && toYYYYMM === null)) {
    const err = new Error("Parámetros inválidos: from/to deben ser YYYY-MM");
    err.status = 400;
    throw err;
  }

  if (fromYYYYMM !== null && toYYYYMM !== null && fromYYYYMM > toYYYYMM) {
    const err = new Error("Rango inválido: from no puede ser mayor que to");
    err.status = 400;
    throw err;
  }

  const { rows } = await pool.query(
    `
    SELECT
      period_year::int,
      period_month::int,
      COUNT(*)::bigint AS filas
    FROM siapp.full_sales
    WHERE ($1::int IS NULL OR period_year = $1)
      AND ($2::int IS NULL OR (period_year * 100 + period_month) >= $2)
      AND ($3::int IS NULL OR (period_year * 100 + period_month) <= $3)
    GROUP BY 1,2
    ORDER BY 1 DESC, 2 DESC
    `,
    [y, fromYYYYMM, toYYYYMM]
  );

  return rows.map((r) => ({
    period_year: Number(r.period_year),
    period_month: Number(r.period_month),
    filas: Number(r.filas),
    period: `${r.period_year}-${String(r.period_month).padStart(2, "0")}`
  }));
}

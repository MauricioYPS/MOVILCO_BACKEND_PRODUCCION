// services/siapp.monthly-sales.summary.service.js
import pool from "../config/database.js";

function normalizePeriodInput(period) {
  if (Array.isArray(period)) return period[0];
  if (period === null || period === undefined) return null;
  return String(period).trim();
}

function parsePeriod(period) {
  const p = normalizePeriodInput(period);
  if (!p) return null;

  const m = p.match(/^(\d{4})-(\d{1,2})$/);
  if (!m) return null;

  const year = Number(m[1]);
  const month = Number(m[2]);

  if (!Number.isFinite(year) || !Number.isFinite(month)) return null;
  if (month < 1 || month > 12) return null;

  return { year, month };
}

export async function getMonthlySalesSummary({
  period,
  advisor_id = null,
  district_mode = "auto"
} = {}) {
  const per = parsePeriod(period);
  if (!per) throw new Error("Falta o es inválido ?period=YYYY-MM");

  const { year, month } = per;

  const advisor = advisor_id != null && String(advisor_id).trim() !== ""
    ? String(advisor_id).trim()
    : null;

  const mode = String(district_mode || "auto").toLowerCase();

  // Selección de distrito usuario según modo
  // auto: COALESCE(district_claro, district)
  // district: district
  // district_claro: district_claro
  const userDistrictExpr =
    mode === "district" ? "u.district"
    : mode === "district_claro" ? "u.district_claro"
    : "COALESCE(u.district_claro, u.district)";

  // Clasificación:
  // - in_district: existe u + ambos distritos y son iguales
  // - out_district: existe u + ambos distritos y son distintos
  // - unclassified: no hay user o falta distrito en alguno
  const q = await pool.query(
    `
    SELECT
      COUNT(*)::int AS total_sales,

      SUM(
        CASE
          WHEN u.id IS NOT NULL
           AND fs.d_distrito IS NOT NULL
           AND ${userDistrictExpr} IS NOT NULL
           AND fs.d_distrito = ${userDistrictExpr}
          THEN 1 ELSE 0
        END
      )::int AS in_district,

      SUM(
        CASE
          WHEN u.id IS NOT NULL
           AND fs.d_distrito IS NOT NULL
           AND ${userDistrictExpr} IS NOT NULL
           AND fs.d_distrito <> ${userDistrictExpr}
          THEN 1 ELSE 0
        END
      )::int AS out_district,

      SUM(
        CASE
          WHEN u.id IS NULL
            OR fs.d_distrito IS NULL
            OR ${userDistrictExpr} IS NULL
          THEN 1 ELSE 0
        END
      )::int AS unclassified,

      SUM(CASE WHEN u.id IS NOT NULL THEN 1 ELSE 0 END)::int AS sales_with_user,
      SUM(CASE WHEN u.id IS NULL THEN 1 ELSE 0 END)::int AS sales_without_user

    FROM siapp.full_sales fs
    LEFT JOIN core.users u
      ON u.document_id::text = fs.idasesor::text
    WHERE fs.period_year = $1
      AND fs.period_month = $2
      AND ($3::text IS NULL OR fs.idasesor::text = $3)
    `,
    [year, month, advisor]
  );

  const row = q.rows[0] || {};

  return {
    period: `${year}-${String(month).padStart(2, "0")}`,
    period_year: year,
    period_month: month,
    advisor_id: advisor,
    district_mode: mode,

    total_sales: Number(row.total_sales || 0),
    in_district: Number(row.in_district || 0),
    out_district: Number(row.out_district || 0),
    unclassified: Number(row.unclassified || 0),

    sales_with_user: Number(row.sales_with_user || 0),
    sales_without_user: Number(row.sales_without_user || 0)
  };
}

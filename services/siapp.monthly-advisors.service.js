// services/siapp.monthly-advisors.service.js
import pool from "../config/database.js";

function parsePeriod(period) {
  const m = String(period || "").trim().match(/^(\d{4})-(\d{2})$/);
  if (!m) return null;
  const year = Number(m[1]);
  const month = Number(m[2]);
  if (month < 1 || month > 12) return null;
  return { year, month };
}

/**
 * Lista mensual unificada de asesores con ventas en el periodo,
 * marcando si está o no en nómina/sistema (existe en core.users).
 *
 * IMPORTANTE:
 * - Se usa SIEMPRE fs.idasesor como llave.
 * - NO se usa cedula_vendedor.
 *
 * Params:
 *  - period: 'YYYY-MM' (requerido)
 *  - q: búsqueda opcional por idasesor o nombreasesor
 *  - limit, offset: paginación
 *  - order: 'ventas_desc' | 'ventas_asc' | 'nombre_asc' | 'nombre_desc'
 *
 * Retorna:
 *  - rows: lista
 *  - total: total de asesores del periodo (sin paginar)
 */
export async function listMonthlyAdvisors({ period, q = null, limit = 200, offset = 0, order = "ventas_desc" }) {
  const per = parsePeriod(period);
  if (!per) throw new Error("Falta o es inválido ?period=YYYY-MM");

  const { year, month } = per;

  const safeLimit = Math.min(Math.max(Number(limit) || 200, 1), 1000);
  const safeOffset = Math.max(Number(offset) || 0, 0);

  // Orden
  let orderBy = "ventas_total DESC";
  if (order === "ventas_asc") orderBy = "ventas_total ASC";
  if (order === "nombre_asc") orderBy = "nombre_asesor_final ASC NULLS LAST";
  if (order === "nombre_desc") orderBy = "nombre_asesor_final DESC NULLS LAST";

  // Filtro búsqueda (idasesor o nombreasesor)
  const hasQ = q && String(q).trim().length > 0;
  const qLike = hasQ ? `%${String(q).trim().toUpperCase()}%` : null;

  // Total sin paginación
  const totalQ = await pool.query(
    `
    WITH base AS (
      SELECT
        fs.idasesor,
        fs.nombreasesor
      FROM siapp.full_sales fs
      WHERE fs.period_year = $1 AND fs.period_month = $2
        AND fs.idasesor IS NOT NULL
        AND ($3::text IS NULL OR
             UPPER(fs.idasesor::text) LIKE $3 OR
             UPPER(COALESCE(fs.nombreasesor,'')) LIKE $3)
      GROUP BY fs.idasesor, fs.nombreasesor
    )
    SELECT COUNT(*)::int AS total
    FROM base
    `,
    [year, month, qLike]
  );

  const total = Number(totalQ.rows[0]?.total || 0);

  // Datos paginados y unificados
  const dataQ = await pool.query(
    `
    WITH base AS (
      SELECT
        fs.idasesor,
        MAX(fs.nombreasesor) AS nombreasesor,
        COUNT(*)::int AS ventas_total,

        -- cantserv es VARCHAR; lo intentamos sumar de forma segura.
        -- Si cantserv no es numérico, suma 0.
        COALESCE(SUM(
          COALESCE(
            NULLIF(regexp_replace(fs.cantserv::text, '[^0-9\\.-]', '', 'g'), '')::numeric,
            0
          )
        ), 0)::numeric AS cantserv_total

      FROM siapp.full_sales fs
      WHERE fs.period_year = $1 AND fs.period_month = $2
        AND fs.idasesor IS NOT NULL
        AND ($3::text IS NULL OR
             UPPER(fs.idasesor::text) LIKE $3 OR
             UPPER(COALESCE(fs.nombreasesor,'')) LIKE $3)
      GROUP BY fs.idasesor
    )
    SELECT
      b.idasesor,
      COALESCE(u.name, b.nombreasesor) AS nombre_asesor_final,
      b.nombreasesor AS nombre_asesor_siapp,

      b.ventas_total,
      b.cantserv_total,

      (u.id IS NOT NULL) AS en_nomina,
      u.id AS user_id,
      u.active AS activo_en_sistema,
      u.district_claro,
      u.district

    FROM base b
    LEFT JOIN core.users u
      ON u.document_id = b.idasesor
    ORDER BY ${orderBy}
    LIMIT $4 OFFSET $5
    `,
    [year, month, qLike, safeLimit, safeOffset]
  );

  return {
    period: `${year}-${String(month).padStart(2, "0")}`,
    period_year: year,
    period_month: month,
    total,
    limit: safeLimit,
    offset: safeOffset,
    rows: dataQ.rows
  };
}

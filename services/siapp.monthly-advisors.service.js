// services/siapp.monthly-advisors.service.js
import pool from "../config/database.js";

function parsePeriod(period) {
  const raw = String(period || "").trim();

  // ACEPTA YYYY-MM o YYYY-M (aditivo, no rompe el caso YYYY-MM)
  const m = raw.match(/^(\d{4})-(\d{1,2})$/);
  if (!m) return null;

  const year = Number(m[1]);
  const month = Number(m[2]);

  if (!Number.isFinite(year) || !Number.isFinite(month)) return null;
  if (month < 1 || month > 12) return null;

  return { year, month };
}

function parseBool(v) {
  if (v === true || v === "true" || v === 1 || v === "1") return true;
  if (v === false || v === "false" || v === 0 || v === "0") return false;
  return null;
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
 *  - period: 'YYYY-MM' (requerido) (también soporta 'YYYY-M')
 *  - q: búsqueda opcional por idasesor o nombreasesor
 *  - limit, offset: paginación
 *  - order: 'ventas_desc' | 'ventas_asc' | 'nombre_asc' | 'nombre_desc'
 *  - only_with_user: true/false (opcional)
 *      - true  => solo asesores que existen en core.users
 *      - false => solo asesores que NO existen en core.users
 *
 * Retorna:
 *  - rows: lista
 *  - total: total de asesores del periodo (con filtros, sin paginar)
 */
export async function listMonthlyAdvisors({
  period,
  q = null,
  limit = 200,
  offset = 0,
  order = "ventas_desc",
  only_with_user = null
}) {
  const per = parsePeriod(period);
  if (!per) throw new Error("Falta o es inválido ?period=YYYY-MM");

  const { year, month } = per;

  const safeLimit = Math.min(Math.max(Number(limit) || 200, 1), 1000);
  const safeOffset = Math.max(Number(offset) || 0, 0);

  // Orden (manteniendo tu lógica; si llega algo raro, cae a ventas_desc)
  let orderBy = "ventas_total DESC";
  if (order === "ventas_asc") orderBy = "ventas_total ASC";
  if (order === "nombre_asc") orderBy = "nombre_asesor_final ASC NULLS LAST";
  if (order === "nombre_desc") orderBy = "nombre_asesor_final DESC NULLS LAST";

  // Filtro búsqueda (idasesor o nombreasesor)
  const hasQ = q && String(q).trim().length > 0;
  const qLike = hasQ ? `%${String(q).trim().toUpperCase()}%` : null;

  // Filtro opcional por existencia en core.users
  const onlyWithUser = parseBool(only_with_user);

  // -------------------------------------------------------------------
  // TOTAL sin paginación (pero SÍ con filtros q y only_with_user)
  // -------------------------------------------------------------------
  const totalQ = await pool.query(
    `
    WITH base AS (
      SELECT
        fs.idasesor::text AS idasesor,
        MAX(fs.nombreasesor) AS nombreasesor
      FROM siapp.full_sales fs
      WHERE fs.period_year = $1 AND fs.period_month = $2
        AND fs.idasesor IS NOT NULL
        AND BTRIM(fs.idasesor::text) <> ''
        AND (
          $3::text IS NULL OR
          UPPER(fs.idasesor::text) LIKE $3 OR
          UPPER(COALESCE(fs.nombreasesor,'')) LIKE $3
        )
      GROUP BY fs.idasesor::text
    )
    SELECT COUNT(*)::int AS total
    FROM base b
    LEFT JOIN core.users u
      ON u.document_id::text = b.idasesor::text
    WHERE
      (
        $4::bool IS NULL OR
        ($4 = true  AND u.id IS NOT NULL) OR
        ($4 = false AND u.id IS NULL)
      )
    `,
    [year, month, qLike, onlyWithUser]
  );

  const total = Number(totalQ.rows[0]?.total || 0);

  // -------------------------------------------------------------------
  // DATA paginada y unificada
  //  - cantserv es VARCHAR; sumamos numérico de forma segura (regex)
  //  - JOIN robusto por texto
  //  - filtro only_with_user aplicado
  // -------------------------------------------------------------------
  const dataQ = await pool.query(
    `
    WITH base AS (
      SELECT
        fs.idasesor::text AS idasesor,
        MAX(fs.nombreasesor) AS nombreasesor,
        COUNT(*)::int AS ventas_total,

        -- cantserv puede venir con símbolos, comas decimales, etc.
        -- Extraemos el primer número válido; si no hay, suma 0.
        COALESCE(SUM(
          COALESCE(
            NULLIF(
              (regexp_match(
                REPLACE(COALESCE(fs.cantserv::text,''), ',', '.'),
                '(-?\\d+(?:\\.\\d+)?)'
              ))[1],
              ''
            )::numeric,
            0
          )
        ), 0)::numeric AS cantserv_total

      FROM siapp.full_sales fs
      WHERE fs.period_year = $1 AND fs.period_month = $2
        AND fs.idasesor IS NOT NULL
        AND BTRIM(fs.idasesor::text) <> ''
        AND (
          $3::text IS NULL OR
          UPPER(fs.idasesor::text) LIKE $3 OR
          UPPER(COALESCE(fs.nombreasesor,'')) LIKE $3
        )
      GROUP BY fs.idasesor::text
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
      ON u.document_id::text = b.idasesor::text
    WHERE
      (
        $6::bool IS NULL OR
        ($6 = true  AND u.id IS NOT NULL) OR
        ($6 = false AND u.id IS NULL)
      )
    ORDER BY ${orderBy}
    LIMIT $4 OFFSET $5
    `,
    [year, month, qLike, safeLimit, safeOffset, onlyWithUser]
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

// services/siapp.monthly-nomina.service.js
import pool from "../config/database.js";

function normalizePeriodInput(period) {
  if (Array.isArray(period)) return period[0];
  if (period === null || period === undefined) return null;
  return String(period).trim();
}

function parsePeriod(period) {
  const p = normalizePeriodInput(period);
  if (!p) return null;

  const m = p.match(/^(\d{4})-(\d{1,2})$/); // acepta YYYY-M o YYYY-MM
  if (!m) return null;

  const year = Number(m[1]);
  const month = Number(m[2]);

  if (!Number.isFinite(year) || !Number.isFinite(month)) return null;
  if (month < 1 || month > 12) return null;

  return { year, month };
}

export async function getMonthlyNominaPreview({ period, q = null, limit = 200, offset = 0 }) {
  const per = parsePeriod(period);
  if (!per) throw new Error("Falta o es inválido ?period=YYYY-MM");

  const { year, month } = per;

  const safeLimit = Math.min(Math.max(Number(limit) || 200, 1), 1000);
  const safeOffset = Math.max(Number(offset) || 0, 0);

  const hasQ = q && String(q).trim().length > 0;
  const qLike = hasQ ? `%${String(q).trim().toUpperCase()}%` : null;

  // Total: nómina(sistema) + asesores fuera sistema (presentes en SIAPP)
  const totalQ = await pool.query(
    `
    WITH nomina_users AS (
      SELECT u.document_id AS cedula, u.name AS nombre
      FROM core.users u
      WHERE ($3::text IS NULL OR
        UPPER(u.document_id::text) LIKE $3 OR
        UPPER(COALESCE(u.name,'')) LIKE $3
      )
    ),
    siapp_unknown AS (
      SELECT fs.idasesor::text AS cedula, MAX(fs.nombreasesor) AS nombre
      FROM siapp.full_sales fs
LEFT JOIN core.users u
  ON regexp_replace(u.document_id::text, '\D', '', 'g')
   = regexp_replace(fs.idasesor::text, '\D', '', 'g')
      WHERE fs.period_year=$1 AND fs.period_month=$2
        AND fs.idasesor IS NOT NULL
        AND u.id IS NULL
        AND ($3::text IS NULL OR
          UPPER(fs.idasesor::text) LIKE $3 OR
          UPPER(COALESCE(fs.nombreasesor,'')) LIKE $3
        )
      GROUP BY fs.idasesor
    )
    SELECT (
      (SELECT COUNT(*) FROM nomina_users)
      + (SELECT COUNT(*) FROM siapp_unknown)
    )::int AS total
    `,
    [year, month, qLike]
  );

  const total = Number(totalQ.rows[0]?.total || 0);

  const dataQ = await pool.query(
    `
    WITH
    -- Nómina / sistema (core.users) + monthly + progress
    nomina AS (
      SELECT
        u.id AS user_id,
        u.document_id::text AS cedula,
        u.name AS nombre_funcionario,

        TRUE AS en_sistema,

        -- CONTRATADO DEL MES = existe registro monthly para el periodo
        (um.id IS NOT NULL) AS contratado_mes,

        u.district AS distrito,
        u.district_claro AS distrito_claro,

        u.contract_start AS fecha_inicio_contrato,
        u.contract_end AS fecha_fin_contrato,

        COALESCE(um.presupuesto_mes, 0)::numeric AS presupuesto_mes,
        COALESCE(um.dias_laborados, 0)::int AS dias_laborados_31,
        COALESCE(um.prorrateo, 0)::numeric AS prorrateo_novedades,

        COALESCE(nov.novedades, '') AS novedades,

        -- Estado mensual (lo que verá negocio)
        CASE
          WHEN um.id IS NULL THEN 'SIN PRESUPUESTO / FUERA NOMINA DEL MES'
          WHEN u.active THEN 'ACTIVO'
          ELSE 'RETIRADO'
        END AS estado_mes,

        -- Garantizados: solo aplica si contratado_mes
        CASE WHEN um.id IS NOT NULL THEN COALESCE(p.expected_count, 0)::numeric ELSE 0::numeric END AS garantizado_para_comisionar,
        CASE WHEN um.id IS NOT NULL THEN COALESCE(p.adjusted_count, 0)::numeric ELSE 0::numeric END AS garantizado_con_novedades,

        -- Ventas: siempre se pueden mostrar (si hay progress)
        COALESCE(p.real_in_count, 0)::int AS ventas_distrito,
        COALESCE(p.real_out_count, 0)::int AS ventas_fuera_distrito,
        COALESCE(p.real_total_count, 0)::int AS total_ventas,

        -- Diferencias: solo tienen sentido si contratado_mes
        CASE WHEN um.id IS NOT NULL
          THEN (COALESCE(p.real_in_count,0) - COALESCE(p.expected_count,0))::numeric
          ELSE 0::numeric
        END AS diferencia_en_distrito,

        CASE WHEN um.id IS NOT NULL
          THEN (COALESCE(p.real_total_count,0) - COALESCE(p.adjusted_count,0))::numeric
          ELSE 0::numeric
        END AS diferencia_total,

        -- Cumple: si no hay monthly => NO APLICA (aunque progress diga met=true por expected=0)
        CASE
          WHEN um.id IS NULL THEN 'NO APLICA'
          WHEN COALESCE(p.met_in_district,false) THEN 'CUMPLE'
          ELSE 'NO CUMPLE'
        END AS cumple_distrito_zonificado,

        CASE
          WHEN um.id IS NULL THEN 'NO APLICA'
          WHEN COALESCE(p.met_global,false) THEN 'CUMPLE'
          ELSE 'NO CUMPLE'
        END AS cumple_global,

        CASE WHEN um.id IS NOT NULL THEN COALESCE(p.compliance_in_percent, 0)::numeric ELSE 0::numeric END AS compliance_in_percent,
        CASE WHEN um.id IS NOT NULL THEN COALESCE(p.compliance_global_percent, 0)::numeric ELSE 0::numeric END AS compliance_global_percent

      FROM core.users u
      LEFT JOIN core.user_monthly um
        ON um.user_id=u.id AND um.period_year=$1 AND um.period_month=$2
      LEFT JOIN core.progress p
        ON p.user_id=u.id AND p.period_year=$1 AND p.period_month=$2
      LEFT JOIN (
        SELECT n.user_id,
          STRING_AGG(
            n.novelty_type || ' ' ||
            to_char(n.start_date,'YYYY-MM-DD') || '→' ||
            to_char(n.end_date,'YYYY-MM-DD'),
            ' | '
          ) AS novedades
        FROM core.user_novelties n
        WHERE (n.start_date, n.end_date)
          OVERLAPS (
            make_date($1,$2,1),
            make_date($1,$2,1) + INTERVAL '1 month - 1 day'
          )
        GROUP BY n.user_id
      ) nov ON nov.user_id=u.id
      WHERE ($3::text IS NULL OR
        UPPER(u.document_id::text) LIKE $3 OR
        UPPER(COALESCE(u.name,'')) LIKE $3
      )
    ),

    -- Fuera de sistema (no existe en core.users) pero aparece en SIAPP
    unknown AS (
      SELECT
        NULL::int AS user_id,
        fs.idasesor::text AS cedula,
        MAX(fs.nombreasesor) AS nombre_funcionario,

        FALSE AS en_sistema,
        FALSE AS contratado_mes,

        'SIN DISTRITO ASIGNADO'::text AS distrito,
        'SIN DISTRITO ASIGNADO'::text AS distrito_claro,

        NULL::date AS fecha_inicio_contrato,
        NULL::date AS fecha_fin_contrato,

        0::numeric AS presupuesto_mes,
        0::int AS dias_laborados_31,
        0::numeric AS prorrateo_novedades,

        ''::text AS novedades,
        'FUERA SISTEMA / HISTORICO'::text AS estado_mes,

        0::numeric AS garantizado_para_comisionar,
        0::numeric AS garantizado_con_novedades,

        0::int AS ventas_distrito,
        COUNT(*)::int AS ventas_fuera_distrito,
        COUNT(*)::int AS total_ventas,

        0::numeric AS diferencia_en_distrito,
        0::numeric AS diferencia_total,

        'NO APLICA'::text AS cumple_distrito_zonificado,
        'NO APLICA'::text AS cumple_global,

        0::numeric AS compliance_in_percent,
        0::numeric AS compliance_global_percent

      FROM siapp.full_sales fs
      LEFT JOIN core.users u ON u.document_id = fs.idasesor
      WHERE fs.period_year=$1 AND fs.period_month=$2
        AND fs.idasesor IS NOT NULL
        AND u.id IS NULL
        AND ($3::text IS NULL OR
          UPPER(fs.idasesor::text) LIKE $3 OR
          UPPER(COALESCE(fs.nombreasesor,'')) LIKE $3
        )
      GROUP BY fs.idasesor
    ),

    unioned AS (
      SELECT * FROM nomina
      UNION ALL
      SELECT * FROM unknown
    )

    SELECT *
    FROM unioned
    ORDER BY
      contratado_mes DESC,          -- primero los contratados del mes
      en_sistema DESC,              -- luego los registrados
      total_ventas DESC,
      nombre_funcionario ASC NULLS LAST
    LIMIT $4 OFFSET $5
    `,
    [year, month, qLike, safeLimit, safeOffset]
  );

  const rows = dataQ.rows.map((r, idx) => ({
    item: safeOffset + idx + 1,

    cedula: r.cedula,
    nombre_funcionario: r.nombre_funcionario,

    // Excel-like
    contratado: r.contratado_mes ? "SI" : "NO",

    // Flags claros para UI
    en_sistema: r.en_sistema,
    contratado_mes: r.contratado_mes,

    distrito: r.distrito,
    distrito_claro: r.distrito_claro,

    fecha_inicio_contrato: r.fecha_inicio_contrato,
    fecha_fin_contrato: r.fecha_fin_contrato,

    novedades: r.novedades,
    estado: r.estado_mes,

    presupuesto_mes: Number(r.presupuesto_mes || 0),
    dias_laborados_31: Number(r.dias_laborados_31 || 0),
    prorrateo_novedades: Number(r.prorrateo_novedades || 0),

    garantizado_para_comisionar: Number(r.garantizado_para_comisionar || 0),
    garantizado_con_novedades: Number(r.garantizado_con_novedades || 0),

    ventas_distrito: Number(r.ventas_distrito || 0),
    ventas_fuera_distrito: Number(r.ventas_fuera_distrito || 0),
    total_ventas: Number(r.total_ventas || 0),

    diferencia_en_distrito: Number(r.diferencia_en_distrito || 0),
    diferencia_total: Number(r.diferencia_total || 0),

    cumple_distrito_zonificado: r.cumple_distrito_zonificado,
    cumple_global: r.cumple_global,

    compliance_in_percent: Number(r.compliance_in_percent || 0),
    compliance_global_percent: Number(r.compliance_global_percent || 0),
  }));

  return {
    period: `${year}-${String(month).padStart(2, "0")}`,
    period_year: year,
    period_month: month,
    total,
    limit: safeLimit,
    offset: safeOffset,
    rows
  };
}

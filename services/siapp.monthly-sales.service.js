// services/siapp.monthly-sales.service.js
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

// Normalización “suave” para comparar distritos sin depender de unaccent() en Postgres
function normTxt(v) {
  if (v == null) return "";
  return String(v)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "") // quita acentos
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
}

function parseBool(v) {
  if (v === true || v === "true" || v === 1 || v === "1") return true;
  if (v === false || v === "false" || v === 0 || v === "0") return false;
  return null;
}

export async function getMonthlySalesDetail({
  period,
  limit = 200,
  offset = 0,
  q = null,
  advisor_id = null,     // idasesor exacto
  only_in = null,        // true/false/null
  district_mode = "auto" // auto|district|district_claro
} = {}) {
  const per = parsePeriod(period);
  if (!per) throw new Error("Falta o es inválido ?period=YYYY-MM");

  const { year, month } = per;

  const safeLimit = Math.min(Math.max(Number(limit) || 200, 1), 2000);
  const safeOffset = Math.max(Number(offset) || 0, 0);

  const hasQ = q && String(q).trim().length > 0;
  const qLike = hasQ ? `%${String(q).trim().toUpperCase()}%` : null;

  const advisor =
    advisor_id != null && String(advisor_id).trim() !== ""
      ? String(advisor_id).trim()
      : null;

  const onlyInParsed = parseBool(only_in);
  const mode = String(district_mode || "auto").toLowerCase();

  // Campo de distrito usuario según modo
  const getDistritoUsuario = (row) => {
    if (mode === "district") return row.user_district || null;
    if (mode === "district_claro") return row.user_district_claro || null;
    return (row.user_district_claro || row.user_district) || null;
  };

  // -------------------------------------------------------------------
  // 1) TOTAL (del filtro base period + q + advisor)
  // -------------------------------------------------------------------
  const baseWhere = `
    fs.period_year = $1 AND fs.period_month = $2
    AND ($3::text IS NULL OR fs.idasesor::text = $3)
    AND (
      $4::text IS NULL OR
      UPPER(COALESCE(fs.idasesor::text,'')) LIKE $4 OR
      UPPER(COALESCE(fs.nombreasesor,'')) LIKE $4 OR
      UPPER(COALESCE(fs.cuenta,'')) LIKE $4 OR
      UPPER(COALESCE(fs.ot,'')) LIKE $4 OR
      UPPER(COALESCE(fs.venta,'')) LIKE $4 OR
      UPPER(COALESCE(fs.d_distrito,'')) LIKE $4
    )
  `;

  const totalQ = await pool.query(
    `
    SELECT COUNT(*)::int AS total
    FROM siapp.full_sales fs
    WHERE ${baseWhere}
    `,
    [year, month, advisor, qLike]
  );

  const total = Number(totalQ.rows[0]?.total || 0);

  // -------------------------------------------------------------------
  // 2) DATA (page)
  //    Nota: ya NO depende de core.user_monthly
  // -------------------------------------------------------------------
  const rowsQ = await pool.query(
    `
    SELECT
      fs.id,
      fs.period_year,
      fs.period_month,
      fs.fecha,

      fs.venta,
      fs.cuenta,
      fs.ot,

      fs.estado_liquidacion,
      fs.linea_negocio,
      fs.tipored,
      fs.division,
      fs.area,
      fs.zona,
      fs.poblacion,

      fs.idasesor,
      fs.nombreasesor,
      fs.d_distrito AS distrito_venta,

      fs.renta,

      fs.tipo_registro,
      fs.estrato,
      fs.paquete_pvd,
      fs.mintic,
      fs.tipo_prodcuto,
      fs.ventaconvergente,
      fs.venta_instale_dth,
      fs.sac_final,

      fs.nombre_regional,
      fs.nombre_comercial,
      fs.nombre_lider,

      fs.modalidad_venta,
      fs.tipo_vendedor,
      fs.tipo_red_comercial,
      fs.tipo_contrato,

      fs.tarifa_venta,
      fs.comision_neta,
      fs.punto_equilibrio,

      fs.source_file,

      u.id AS user_id,
      u.document_id AS user_document_id,
      u.name AS user_name,
      u.district AS user_district,
      u.district_claro AS user_district_claro

    FROM siapp.full_sales fs
    LEFT JOIN core.users u
      ON u.document_id::text = fs.idasesor::text

    WHERE ${baseWhere}

    ORDER BY fs.fecha ASC, fs.id ASC
    LIMIT $5 OFFSET $6
    `,
    [year, month, advisor, qLike, safeLimit, safeOffset]
  );

  // -------------------------------------------------------------------
  // 3) Transformación + clasificación in/out
  //    - Si advisor_id está presente => devolvemos header advisor + sales[]
  //    - Si no => devolvemos rows[] (cada venta con flags básicos)
  // -------------------------------------------------------------------
  let pageIn = 0;
  let pageOut = 0;
  let pageUnclassified = 0;

  // Header advisor (solo si advisor_id)
  let advisorHeader = null;

  const sales = [];
  const listRows = [];

  for (const r of rowsQ.rows) {
    const distritoVenta = r.distrito_venta;
    const distritoUsuario = getDistritoUsuario(r);

    let inDistrict = null;
    if (distritoUsuario && distritoVenta) {
      inDistrict = normTxt(distritoUsuario) === normTxt(distritoVenta);
    }

    // Filtro only_in/out aplicado a nivel de página
    if (onlyInParsed === true && inDistrict !== true) continue;
    if (onlyInParsed === false && inDistrict !== false) continue;

    if (inDistrict === true) pageIn += 1;
    else if (inDistrict === false) pageOut += 1;
    else pageUnclassified += 1;

    const enNomina = !!r.user_id;

    // Si viene advisor_id, definimos header una sola vez (usando primera fila útil)
    if (advisor && !advisorHeader) {
      advisorHeader = {
        idasesor: r.idasesor,
        nombreasesor: r.nombreasesor,

        // Regla nueva: contratado = existe en core.users (nómina actual)
        en_nomina: enNomina,
        user_id: r.user_id || null,
        user_document_id: r.user_document_id || null,
        user_name: r.user_name || null,

        distrito_usuario: distritoUsuario || null,
        distrito_usuario_raw: {
          district: r.user_district || null,
          district_claro: r.user_district_claro || null
        },

        // Para UI / contexto
        period: `${year}-${String(month).padStart(2, "0")}`,
        district_mode: mode
      };
    }

    // Base venta (sin repetir info del asesor)
    const saleItem = {
      id: r.id,
      fecha: r.fecha,
      period: `${r.period_year}-${String(r.period_month).padStart(2, "0")}`,

      distrito_venta: r.distrito_venta,
      in_district: inDistrict, // true/false/null

      venta: r.venta,
      cuenta: r.cuenta,
      ot: r.ot,

      estado_liquidacion: r.estado_liquidacion,
      linea_negocio: r.linea_negocio,
      tipored: r.tipored,
      division: r.division,
      area: r.area,
      zona: r.zona,
      poblacion: r.poblacion,
      renta: r.renta,

      tipo_registro: r.tipo_registro,
      estrato: r.estrato,
      paquete_pvd: r.paquete_pvd,
      mintic: r.mintic,
      tipo_prodcuto: r.tipo_prodcuto,
      ventaconvergente: r.ventaconvergente,
      venta_instale_dth: r.venta_instale_dth,
      sac_final: r.sac_final,

      nombre_regional: r.nombre_regional,
      nombre_comercial: r.nombre_comercial,
      nombre_lider: r.nombre_lider,

      modalidad_venta: r.modalidad_venta,
      tipo_vendedor: r.tipo_vendedor,
      tipo_red_comercial: r.tipo_red_comercial,
      tipo_contrato: r.tipo_contrato,

      tarifa_venta: r.tarifa_venta,
      comision_neta: r.comision_neta,
      punto_equilibrio: r.punto_equilibrio,

      source_file: r.source_file
    };

    if (advisor) {
      // Modo ASESOR: sales[] (sin repetir user info)
      sales.push(saleItem);
    } else {
      // Modo LISTA: sí necesitamos algunos campos del asesor para tabla global
      listRows.push({
        ...saleItem,

        idasesor: r.idasesor,
        nombreasesor: r.nombreasesor,

        en_nomina: enNomina,
        distrito_usuario: distritoUsuario || null
      });
    }
  }

  const responseBase = {
    period: `${year}-${String(month).padStart(2, "0")}`,
    period_year: year,
    period_month: month,
    total, // total del filtro base (sin only_in/out global)
    limit: safeLimit,
    offset: safeOffset,
    page_counts: {
      page_rows: advisor ? sales.length : listRows.length,
      in_district: pageIn,
      out_district: pageOut,
      unclassified: pageUnclassified,
      district_mode: mode,
      only_in: onlyInParsed
    }
  };

  if (advisor) {
    // Si no hubo ninguna fila útil (por q/only_in), igual devolvemos header mínimo
    if (!advisorHeader) {
      advisorHeader = {
        idasesor: advisor,
        nombreasesor: null,
        en_nomina: null,
        user_id: null,
        user_document_id: null,
        user_name: null,
        distrito_usuario: null,
        distrito_usuario_raw: { district: null, district_claro: null },
        period: `${year}-${String(month).padStart(2, "0")}`,
        district_mode: mode
      };
    }

    return {
      ...responseBase,
      advisor: advisorHeader,
      sales
    };
  }

  return {
    ...responseBase,
    rows: listRows
  };
}


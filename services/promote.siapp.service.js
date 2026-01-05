// services/promote.siapp.service.js
import pool from "../config/database.js";
import { loadSettings } from "./settings.service.js";

/**
 * promoteSiappFromFullSales
 * Calcula métricas IN / OUT / UNZONED / KPIs para cada asesor
 * usando el SIAPP FULL (siapp.full_sales)
 * y guarda resultados en core.progress.
 *
 * REGLAS:
 *  - Match SIEMPRE por IDASESOR (fs.idasesor <-> core.users.document_id)
 *  - KPI vs metas (expected/adjusted) se mide por FILAS (1 fila = 1 conexión/venta).
 *  - IN: distrito de venta == distrito del usuario (canonizado)
 *  - OUT: distrito de venta != distrito del usuario, PERO el distrito de venta es "registrado" (canonizado)
 *  - UNZONED: distrito de venta vacío o NO "registrado" (canonizado)
 *
 * Importante:
 *  - "Registrado" = existe en core.users (district o district_claro) o en tu diccionario STD/ALIAS (targets).
 */

// ======================================================================
// Diccionarios de distritos (los que me pasaste)
// ======================================================================
// =========================
// 1) ESTÁNDARES CANÓNICOS
// =========================
// Nota: aquí definimos los "nombres finales" (canon) que tu sistema entiende.
// Por tu decisión: HUILA canon = "HUILA 6".
// Para cargos/directivos, los dejamos tal cual como vienen (canon igual al texto oficial)
// porque también existen en core.users según tu query.

const DISTRICT_STD = {
  // --- Cargos / Dirección ---
  "GERENTE COMERCIAL": "GERENTE COMERCIAL",
  "DIRECTOR OPERATIVO FIJO NACIONAL": "DIRECTOR OPERATIVO FIJO NACIONAL",
  "DIRECTOR META": "META",
  "DIRECTOR SANTANDER": "SANTANDER",
  "DIRECTOR NORTE SANTANDER, TOLIMA Y HUILA": "NORTE SANTANDER",
  "TOLIMA Y HUILA": "TOLIMA-HUILA",
  "DIRECTOR MEDELLIN OCCIDENTAL": "MEDELLIN OCCIDENTAL",
  "DIRECTOR MEDELLIN NOROCCIDENTAL": "MEDELLIN NOROCCIDENTE",
  "DIRECTOR CALI Y CAUCA": "CALI-CAUCA",

  // --- Medellín / Bello (oficial Excel) ---
  "MEDELLIN": "MEDELLIN",
  "BELLO METROPOLITANO": "BELLO METROPOLITANO",
  "BELLO NORTE": "BELLO NORTE",
  "MEDELLIN OCCIDENTAL 4": "MEDELLIN OCCIDENTAL 4",
  "MEDELLIN NOROCCIDENTE 1": "MEDELLIN NOROCCIDENTE 1",
  "MEDELLIN NOROCCIDENTE 2": "MEDELLIN NOROCCIDENTE 2",

  // --- Cali/Cauca (oficial Excel) ---
  "CAUCA DISTRITO 1": "CAUCA 1",
  "CAUCA 1": "CAUCA 1",
  "ALFONSO LOPEZ": "ALFONSO LOPEZ",
  "CHIPICHAPE": "CHIPICHAPE",
  "COMUNEROS": "COMUNEROS",
  "FLORALIA": "FLORALIA",
  "EL JARDIN": "EL JARDIN",
  "YUMBO": "YUMBO",

  // --- Huila (tu estándar final) ---
  "HUILA 6": "HUILA 6",
  "HUILA DISTRITO 6": "HUILA 6", // lo aceptamos como sinónimo, pero canon final queda HUILA 6

  // --- Pitalito (en users aparece PITALITO, en Excel aparece PITALITO DISTRITO 1) ---
  "PITALITO": "PITALITO",
  "PITALITO DISTRITO 1": "PITALITO",

  // --- Tolima (en users existe TOLIMA 2.1 / 2.2 / 3 y también variantes con coma) ---
  "TOLIMA 2.1": "TOLIMA 2.1",
  "TOLIMA 2.2": "TOLIMA 2.2",
  "TOLIMA 3": "TOLIMA 3",

  // --- Norte de Santander (oficial Excel) ---
  "CUCUTA 1": "CUCUTA 1",
  "CUCUTA 3": "CUCUTA 3",
  "LOS PATIOS": "LOS PATIOS",
  "VILLA DEL ROSARIO": "VILLA DEL ROSARIO",

  // --- Llanos (ojo: SIAPP trae LLANOS 1; nómina trae LLANOS 1.1 y 1.2) ---
  "LLANOS 1.1": "LLANOS 1.1",
  "LLANOS 1.2": "LLANOS 1.2",
  "LLANOS 1": "LLANOS 1", // existe en users según tu conteo (LLANOS 1 cnt 4)

  // --- Villavicencio (oficial Excel) ---
  "VILLAVICENCIO 1": "VILLAVICENCIO 1",
  "VILLAVICENCIO 2": "VILLAVICENCIO 2",
  "VILLAVICENCIO 3": "VILLAVICENCIO 3",

  // --- Santander (B/manga) ---
  "BUCARAMANGA 1": "BUCARAMANGA 1",
  "BUCARAMANGA 2": "BUCARAMANGA 2",
  // BUCARAMANGA 3 existe en users (cnt 4) y en SIAPP (ventas 155), lo dejamos como canon válido
  "BUCARAMANGA 3": "BUCARAMANGA 3",
  "FLORIDABLANCA 1": "FLORIDABLANCA 1",
  "FLORIDABLANCA 2": "FLORIDABLANCA 2",

  // --- Ocaña (users tiene encoding raro; canon lo dejamos como OCANA) ---
  "OCANA": "OCANA",

  // --- Valle Grande (existe en users y en Excel) ---
  "VALLE GRANDE": "VALLE GRANDE",
};


// =========================
// 2) ALIAS / NORMALIZACIONES
// =========================
// Nota: aquí ponemos TODAS las variantes que llegan de SIAPP o de datos sucios de users,
// y las llevamos a un valor "base" (que luego debe existir en DISTRICT_STD o en users).
// Importante: NO mapeamos "GRANADA META" porque NO es oficial (queda unzoned).

const DISTRICT_ALIAS = {
  // -------------------------
  // A) Variantes de "ZONA ..."
  // -------------------------
  "ZONA LLANOS 1.1": "LLANOS 1.1",
  "ZONA LLANOS 1.2": "LLANOS 1.2",

  "ZONA V/CIO 1": "VILLAVICENCIO 1",
  "ZONA V/CIO 2": "VILLAVICENCIO 2",
  "ZONA V/CIO 3": "VILLAVICENCIO 3",

  "ZONA VILLAVICENCIO 1": "VILLAVICENCIO 1",
  "ZONA VILLAVICENCIO 2": "VILLAVICENCIO 2",
  "ZONA VILLAVICENCIO 3": "VILLAVICENCIO 3",

  "ZONA CAUCA 1": "CAUCA 1",
  "ZONA CAUCA 4": "CAUCA 1", // decisión de negocio previa + tu evidencia de match

  "ZONA CHIPICHAPE": "CHIPICHAPE",
  "ZONA COMUNEROS": "COMUNEROS",
  "ZONA YUMBO": "YUMBO",
  "ZONA ALFONSO LOPEZ": "ALFONSO LOPEZ",

  // Huila: alias a tu estándar final
  "ZONA HUILA 6": "HUILA 6",
  "ZONA GARZON": "HUILA 6", // GARZON pertenece a HUILA 6 por tu definición final

  // Tolima: zonas
  "ZONA TOLIMA 2.1": "TOLIMA 2.1",
  "ZONA TOLIMA 2.2": "TOLIMA 2.2",
  "ZONA TOLIMA 3": "TOLIMA 3",

  // Tolima con sufijos de municipio (Excel)
  "ZONA TOLIMA 2.1 (ESPINAL)": "TOLIMA 2.1",
  "ZONA TOLIMA 2.1 (GUAMO)": "TOLIMA 2.1",
  "ZONA TOLIMA 2.2 (FLANDES)": "TOLIMA 2.2",
  "ZONA TOLIMA 2.2 (MELGAR)": "TOLIMA 2.2",
  "ZONA TOLIMA 3 (IBAGUE)": "TOLIMA 3",

  // Norte Santander: zonas
  "ZONA CUCUTA 1": "CUCUTA 1",
  "ZONA CUCUTA 3": "CUCUTA 3",
  "ZONA PATIOS": "LOS PATIOS",
  "ZONA VILLA DEL ROSARIO": "VILLA DEL ROSARIO",

  // Santander: zonas abreviadas (B/manga)
  "ZONA B/MANGA 1": "BUCARAMANGA 1",
  "ZONA B/MANGA 2": "BUCARAMANGA 2",
  "ZONA B/MANGA 3": "BUCARAMANGA 3",

  // Floridablanca zonas
  "ZONA FLORIDABLANCA 1": "FLORIDABLANCA 1",
  "ZONA FLORIDABLANCA 2": "FLORIDABLANCA 2",

  // Cali/Cauca sub-zonas
  "ZONA FLORALIA": "FLORALIA",
  "ZONA EL JARDIN": "EL JARDIN",

  // Medellín y Bello
  "ZONA MEDELLIN - BELLO METROP": "BELLO METROPOLITANO",
  "ZONA MEDELLIN - BELLO METROPOLITANO": "BELLO METROPOLITANO",
  "ZONA MEDELLIN - BELLO NORTE": "BELLO NORTE",

  "ZONA MEDELLIN - NOROCC 1": "MEDELLIN NOROCCIDENTE 1",
  "ZONA MEDELLIN - NOROCC 2": "MEDELLIN NOROCCIDENTE 2",

  // En Excel existe "ZONA MEDELLIN - NOROCC 4" y "ZONA MEDELLIN - OCC 2",
  // pero NO existen como distritos oficiales canónicos (solo aparecen como zonas).
  // Por tu regla: NO debemos forzar mapeo a un distrito oficial distinto.
  // Aun así, estos alias los mantenemos "alineados" a su texto SIAPP/Excel cuando aplique.
  // (Si tu pipeline primero alias->canon y luego valida contra STD/USERS,
  // estos dos quedarán unzoned salvo que tú decidas una regla adicional.)
  "ZONA MEDELLIN - NOROCC 4": "MEDELLIN NOROCCIDENTE 4",
  "ZONA MEDELLIN - OCC 2": "MEDELLIN OCCIDENTAL 2",

  "ZONA MEDELLIN - OCC 4": "MEDELLIN OCCIDENTAL 4",

  // Si en users existe "ZONA MEDELLIN" (cnt 1), lo reducimos a "MEDELLIN"
  "ZONA MEDELLIN": "MEDELLIN",

  // Ocaña: zona (y encoding)
  "ZONA OCAÑA": "OCANA",

  // Valle Grande zona
  "ZONA VALLE GRANDE": "VALLE GRANDE",

  // Pitalito zona (y trailing spaces observados)
  "ZONA PITALITO": "PITALITO",
  "ZONA PITALITO ": "PITALITO",

  // -------------------------
  // B) Variantes SIAPP "DISTRITO ..."
  // -------------------------
  // Tolima SIAPP
  "TOLIMA DISTRITO 2.1": "TOLIMA 2.1",
  "TOLIMA DISTRITO 2.2": "TOLIMA 2.2",
  "TOLIMA DISTRITO 3": "TOLIMA 3",

  // Cauca SIAPP (no oficial en nómina, pero se mapea a CAUCA 1 por tu decisión previa)
  "CAUCA DISTRITO 4": "CAUCA 1",

  // Pitalito SIAPP
  "PITALITO DISTRITO 1": "PITALITO",

  // Huila SIAPP: OJO, tu estándar final es HUILA 6; pero HUILA DISTRITO 1/2 no existen en users/canon.
  // Si llegan, deben quedar unzoned, así que NO los mapeo.

  // -------------------------
  // C) Variantes SIAPP "DTH"
  // -------------------------
  "VILLAVICENCIO DTH 2": "VILLAVICENCIO 2",
  // "BUCARAMANGA 3 DTH" aparece en SIAPP pero no es parte del Excel oficial;
  // Aun así, BUCARAMANGA 3 existe en users, así que lo normalizamos a BUCARAMANGA 3.
  "BUCARAMANGA 3 DTH": "BUCARAMANGA 3",

  // -------------------------
  // D) Normalizaciones por encoding / caracteres raros (BD)
  // -------------------------
  "GARZËN": "GARZON",
  "GARZON": "HUILA 6", // tu estándar final

  "OCAÐA": "OCANA",
  "ZONA OCAÐA": "OCANA",

  // -------------------------
  // E) Normalizaciones por coma decimal (BD)
  // -------------------------
  "TOLIMA 2,1": "TOLIMA 2.1",
  "TOLIMA 2,2": "TOLIMA 2.2",

  // -------------------------
  // F) Normalizaciones menores (espacios / equivalencias)
  // -------------------------
  "ZONA MEDELLIN - OCC 2": "MEDELLIN OCCIDENTAL 2",
  "ZONA MEDELLIN - NOROCC 4": "MEDELLIN NOROCCIDENTE 4",
};



// ======================================================================
// Normalización y canonización (distritos)
// ======================================================================
function normKey(raw) {
  if (!raw) return "";
  let t = String(raw)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "") // quita tildes
    .trim()
    .toUpperCase();

  // Normaliza separadores sin "destruir" la semántica (NO removemos palabras como ZONA/DISTRITO)
  t = t.replace(/\s+/g, " ");
  t = t.replace(/\s*-\s*/g, " - ");
  t = t.replace(/\s*\/\s*/g, "/");
  t = t.replace(/\s*\.\s*/g, ".");
  t = t.replace(/\s*,\s*/g, ",");

  // Limpieza ligera de caracteres raros -> espacio
  // (dejamos letras, números, espacio y estos símbolos: . / - ,)
  t = t.replace(/[^A-Z0-9 .\/\-,]/g, " ").replace(/\s+/g, " ").trim();

  return t;
}

function canonizeDistrict(raw) {
  if (!raw) return "";

  // 1) normalización base
  let t = normKey(raw);
  if (!t) return "";

  // helper: intenta resolver SOLO por alias/std
  const resolveByDict = (k) => {
    const kk = normKey(k);
    if (!kk) return "";
    if (DISTRICT_ALIAS[kk]) return normKey(DISTRICT_ALIAS[kk]);
    if (DISTRICT_STD[kk]) return normKey(DISTRICT_STD[kk]);
    return "";
  };

  // 2) match directo
  {
    const r = resolveByDict(t);
    if (r) return r;
  }

  // 3) regla conocida: V/CIO -> VILLAVICENCIO
  if (t.includes("V/CIO")) {
    const t2 = normKey(t.replace(/V\/CIO/g, "VILLAVICENCIO"));
    const r2 = resolveByDict(t2);
    if (r2) return r2;
    t = t2;
  }

  // 4) quitar prefijo ZONA (heurística segura) y reintentar diccionarios
  const tNoZona = normKey(t.replace(/^ZONA\s+/, "").trim());
  if (tNoZona && tNoZona !== t) {
    const r3 = resolveByDict(tNoZona);
    if (r3) return r3;
  }

  // 5) Transformaciones CONTROLADAS (solo si luego cae en diccionarios)
  // 5.1) coma decimal -> punto (TOLIMA 2,1)
  {
    const tComa = normKey(t.replace(/,/g, "."));
    if (tComa !== t) {
      const r = resolveByDict(tComa);
      if (r) return r;
    }
  }

  // 5.2) quitar DTH (BUCARAMANGA 3 DTH)
  if (/\bDTH\b/.test(t)) {
    const tNoDth = normKey(t.replace(/\bDTH\b/g, "").replace(/\s+/g, " ").trim());
    const r = resolveByDict(tNoDth);
    if (r) return r;
  }

  // 5.3) regla SIAPP: "... DISTRITO <num>" -> dos posibilidades:
  //      A) convertir a "<base> <num>" (TOLIMA DISTRITO 2.2 -> TOLIMA 2.2)
  //      B) quitar por completo (PITALITO DISTRITO 1 -> PITALITO)
  if (/\bDISTRITO\b/.test(t)) {
    // A) DISTRITO numérico al final
    const m = t.match(/^(.*)\s+DISTRITO\s+([0-9]+(\.[0-9]+)?)\s*$/);
    if (m) {
      const base = normKey(m[1]);
      const num = m[2];
      const tNum = normKey(`${base} ${num}`);
      const rA = resolveByDict(tNum);
      if (rA) return rA;

      const rB = resolveByDict(base);
      if (rB) return rB;
    } else {
      // B) si no matchea el patrón estricto, solo intentamos eliminar la palabra DISTRITO
      const tNoWord = normKey(t.replace(/\bDISTRITO\b/g, "").replace(/\s+/g, " ").trim());
      const r = resolveByDict(tNoWord);
      if (r) return r;
    }
  }

  // 5.4) encoding puntual (si normKey no aplica unaccent)
  // (esto es “seguro” porque igualmente re-cae en diccionarios)
  {
    const tFix = normKey(
      t
        .replace(/Ð/g, "N")
        .replace(/Ë/g, "E")
    );
    if (tFix !== t) {
      const r = resolveByDict(tFix);
      if (r) return r;
    }
  }

  // 6) fallback: queda como viene normalizado (esto NO lo "sonifica")
  return t;
}

// ======================================================================
// promote
// ======================================================================
export async function promoteSiappFromFullSales({ period_year, period_month }) {
  const client = await pool.connect();

  // Parse robusto de cantserv (VARCHAR)
  const parseCantServ = (v) => {
    if (v === null || v === undefined) return 0;
    const s = String(v).trim();
    if (!s) return 0;
    const normalized = s.replace(",", ".");
    const n = Number(normalized);
    if (Number.isFinite(n)) return n;
    const m = normalized.match(/-?\d+(\.\d+)?/);
    return m ? Number(m[0]) : 0;
  };

  // Normaliza keys (IDASESOR y document_id)
  const normId = (x) => {
    if (x === null || x === undefined) return "";
    return String(x).trim().replace(/\D+/g, "");
  };

  // Parse numérico robusto para settings (KV string)
  const toNumber = (v, fallback = 0) => {
    if (v === null || v === undefined) return fallback;
    const n = Number(String(v).trim().replace(",", "."));
    return Number.isFinite(n) ? n : fallback;
  };

  try {
    await client.query("BEGIN");

    // 1) Configuración global
    const settings = await loadSettings(client);
    const threshold = toNumber(settings?.compliance_threshold_percent, 100);

    // 2) Ventas reales desde siapp.full_sales
    const { rows: sales } = await client.query(
      `
      SELECT
        fs.idasesor     AS id_asesor,
        fs.nombreasesor AS nombre_asesor,
        fs.d_distrito   AS distrito_venta,
        fs.cantserv     AS cantserv
      FROM siapp.full_sales fs
      WHERE fs.period_year = $1
        AND fs.period_month = $2
        AND fs.idasesor IS NOT NULL
      `,
      [period_year, period_month]
    );

    // 3) Agrupar ventas por asesor (IDASESOR)
    const asesores = {};
    for (const s of sales) {
      const key = normId(s.id_asesor);
      if (!key) continue;

      if (!asesores[key]) {
        asesores[key] = {
          id_asesor: key,
          nombre_asesor: s.nombre_asesor || null,
          ventas: []
        };
      }
      asesores[key].ventas.push(s);
    }

    // 4) Obtener usuarios (match por document_id)
    const { rows: users } = await client.query(
      `
      SELECT id, document_id AS id_asesor, district_claro, district
      FROM core.users
      WHERE document_id IS NOT NULL
      `
    );

    const userMap = {};
    for (const u of users) {
      const key = normId(u.id_asesor);
      if (!key) continue;
      userMap[key] = u;
    }

    // 5) Construir set de distritos "registrados" (canonizados)
    //    Fuente:
    //      - core.users (district / district_claro)
    //      - valores estándar (DISTRICT_STD)
    //      - targets de alias (DISTRICT_ALIAS values)
    const registeredSet = new Set();

    // 5.1 desde usuarios
    const { rows: regRows } = await client.query(
      `
      SELECT DISTINCT d
      FROM (
        SELECT district AS d
        FROM core.users
        WHERE district IS NOT NULL AND TRIM(district) <> ''

        UNION ALL

        SELECT district_claro AS d
        FROM core.users
        WHERE district_claro IS NOT NULL AND TRIM(district_claro) <> ''
      ) x
      `
    );

    for (const r of regRows) {
      const c = canonizeDistrict(r.d);
      if (c) registeredSet.add(c);
    }

    // 5.2 desde diccionarios
    for (const k of Object.keys(DISTRICT_STD)) {
      const c = canonizeDistrict(k);
      if (c) registeredSet.add(c);
    }
    for (const v of Object.values(DISTRICT_STD)) {
      const c = canonizeDistrict(v);
      if (c) registeredSet.add(c);
    }
    for (const k of Object.keys(DISTRICT_ALIAS)) {
      const c = canonizeDistrict(k);
      if (c) registeredSet.add(c);
    }
    for (const v of Object.values(DISTRICT_ALIAS)) {
      const c = canonizeDistrict(v);
      if (c) registeredSet.add(c);
    }

    // 6) Procesar asesores y UPSERT en core.progress
    let upserted = 0;
    let matchedUsers = 0;

    // stats opcionales
    let totalIn = 0;
    let totalOut = 0;
    let totalUnzoned = 0;

    for (const asesor_id of Object.keys(asesores)) {
      const data = asesores[asesor_id];
      const u = userMap[asesor_id];

      if (!u) continue; // asesor no existe en usuarios
      matchedUsers++;

      const ventas = data.ventas;

      // 6.1 Calcular IN / OUT / UNZONED (KPI por filas)
      let real_in = 0;
      let real_out = 0;
      let real_unzoned = 0;

      // Analítica cantserv (no guardamos en progress)
      let cantserv_in = 0;
      let cantserv_out = 0;
      let cantserv_unzoned = 0;

      const d_user_raw = (u.district_claro || u.district || "").trim();
      const d_user = canonizeDistrict(d_user_raw);

      for (const v of ventas) {
        const d_sale_raw = (v.distrito_venta || "").trim();
        const d_sale = canonizeDistrict(d_sale_raw);
        const c = parseCantServ(v.cantserv);

        // Si sale vacío o no está en distritos registrados => UNZONED
        if (!d_sale || !registeredSet.has(d_sale)) {
          real_unzoned += 1;
          cantserv_unzoned += c;
          continue;
        }

        // Si el usuario no tiene distrito (extraño), cualquier venta registrada la mandamos a OUT
        if (!d_user) {
          real_out += 1;
          cantserv_out += c;
          continue;
        }

        // IN si coincide
        if (d_sale === d_user) {
          real_in += 1;
          cantserv_in += c;
        } else {
          real_out += 1;
          cantserv_out += c;
        }
      }

      const real_total = real_in + real_out + real_unzoned;

      totalIn += real_in;
      totalOut += real_out;
      totalUnzoned += real_unzoned;

      // 6.2 user_monthly
      const { rows: umRows } = await client.query(
        `
        SELECT presupuesto_mes, dias_laborados, prorrateo
        FROM core.user_monthly
        WHERE user_id = $1
          AND period_year = $2
          AND period_month = $3
        LIMIT 1
        `,
        [u.id, period_year, period_month]
      );

      let expected = 0;
      let adjusted = 0;

      if (umRows.length > 0) {
        expected = Number(umRows[0].presupuesto_mes || 0);
        adjusted = Number(umRows[0].prorrateo ?? expected);
      }

      // 6.3 Cumplimiento
      const compliance_in =
        adjusted > 0 ? Number(((real_in / adjusted) * 100).toFixed(2)) : 0;

      // Global usa total (incluye unzoned, porque al final son ventas válidas)
      const compliance_global =
        expected > 0 ? Number(((real_total / expected) * 100).toFixed(2)) : 0;

      // Flags
      const met_in = adjusted > 0 && compliance_in >= threshold;
      const met_global = expected > 0 && compliance_global >= threshold;

      // 6.4 UPSERT progress (incluye real_unzoned_count)
      await client.query(
        `
        INSERT INTO core.progress (
          user_id, period_year, period_month,
          real_in_count, real_out_count, real_unzoned_count, real_total_count,
          expected_count, adjusted_count,
          compliance_in_percent, compliance_global_percent,
          met_in_district, met_global,
          created_at, updated_at
        )
        VALUES (
          $1,$2,$3,
          $4,$5,$6,$7,
          $8,$9,
          $10,$11,
          $12,$13,
          NOW(), NOW()
        )
        ON CONFLICT (user_id, period_year, period_month)
        DO UPDATE SET
          real_in_count = EXCLUDED.real_in_count,
          real_out_count = EXCLUDED.real_out_count,
          real_unzoned_count = EXCLUDED.real_unzoned_count,
          real_total_count = EXCLUDED.real_total_count,
          expected_count = EXCLUDED.expected_count,
          adjusted_count = EXCLUDED.adjusted_count,
          compliance_in_percent = EXCLUDED.compliance_in_percent,
          compliance_global_percent = EXCLUDED.compliance_global_percent,
          met_in_district = EXCLUDED.met_in_district,
          met_global = EXCLUDED.met_global,
          updated_at = NOW()
        `,
        [
          u.id,
          period_year,
          period_month,
          real_in,
          real_out,
          real_unzoned,
          real_total,
          expected,
          adjusted,
          compliance_in,
          compliance_global,
          met_in,
          met_global
        ]
      );

      upserted++;

      // Si quieres debug por asesor:
      // console.log({ asesor_id, d_user_raw, d_user, real_in, real_out, real_unzoned, real_total, cantserv_in, cantserv_out, cantserv_unzoned });
    }

    await client.query("COMMIT");

    return {
      ok: true,
      period_year,
      period_month,
      threshold_percent: threshold,
      total_sales_rows: sales.length,
      total_asesores_en_siapp: Object.keys(asesores).length,
      matched_users: matchedUsers,
      upserted,
      totals: {
        in_cnt: totalIn,
        out_cnt: totalOut,
        unzoned_cnt: totalUnzoned,
        total_cnt: totalIn + totalOut + totalUnzoned
      },
      registered_districts: registeredSet.size
    };
  } catch (e) {
    await client.query("ROLLBACK");
    console.error("[PROMOTE_SIAPP_PROGRESS]", e);
    throw e;
  } finally {
    client.release();
  }
}

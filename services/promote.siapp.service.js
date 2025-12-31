// services/promote.siapp.service.js
import pool from "../config/database.js";
import { loadSettings } from "./settings.service.js";

/**
 * promoteSiappFromFullSales
 * Calcula métricas IN / OUT / KPIs para cada asesor
 * usando el SIAPP FULL (siapp.full_sales)
 * y guarda resultados en core.progress.
 *
 * REGLA (CORREGIDA):
 *  - Match SIEMPRE por IDASESOR (fs.idasesor <-> core.users.document_id)
 *  - NO usar cedula_vendedor para nada de matching.
 *  - KPI vs metas (expected/adjusted) se mide por FILAS (1 fila = 1 conexión/venta).
 *  - cantserv se conserva solo como analítica, NO para metas.
 */
export async function promoteSiappFromFullSales({ period_year, period_month }) {
  const client = await pool.connect();

  // Parse robusto de cantserv (VARCHAR)
  const parseCantServ = (v) => {
    if (v === null || v === undefined) return 0;
    const s = String(v).trim();
    if (!s) return 0;

    // Normaliza coma decimal
    const normalized = s.replace(",", ".");
    const n = Number(normalized);
    if (Number.isFinite(n)) return n;

    // Fallback: extraer primer número del string
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

    // 1. Leer configuración global (NO tocamos tu forma actual)
    const settings = await loadSettings(client);

    // Opción 1: usar compliance_threshold_percent como umbral
    // default 100 si no existe
    const threshold = toNumber(settings?.compliance_threshold_percent, 100);

    // 2. Obtener ventas reales desde siapp.full_sales
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

    // Agrupar por asesor (IDASESOR)
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

    // 3. Obtener usuarios reales (match por document_id = IDASESOR)
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

    // 4. Procesar cada asesor y hacer UPSERT en core.progress
    let upserted = 0;
    let matchedUsers = 0;

    for (const asesor_id of Object.keys(asesores)) {
      const data = asesores[asesor_id];
      const u = userMap[asesor_id];

      if (!u) continue; // asesor no existe en usuarios (fuera nómina/presupuesto)
      matchedUsers++;

      const ventas = data.ventas;

      // 4.1 Calcular IN / OUT (KPI POR FILAS)
      let real_in = 0;
      let real_out = 0;

      // Analítica (NO afecta KPI): suma cantserv por si te sirve
      // (no lo guardamos en core.progress, solo queda disponible para debug)
      let cantserv_in = 0;
      let cantserv_out = 0;

      const d_user = (u.district_claro || u.district || "").trim().toUpperCase();

      for (const v of ventas) {
        const d_venta = (v.distrito_venta || "").trim().toUpperCase();
        const c = parseCantServ(v.cantserv);

        const isIn = d_user && d_venta && d_venta === d_user;

        // KPI por filas (1 venta = 1)
        if (isIn) real_in += 1;
        else real_out += 1;

        // Analítica cantserv
        if (isIn) cantserv_in += c;
        else cantserv_out += c;
      }

      const real_total = real_in + real_out;

      // 4.2 Tomar información mensual desde user_monthly (si existe)
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
        // prorrateo puede ser null; si es null, usamos expected
        adjusted = Number(umRows[0].prorrateo ?? expected);
      }

      // 4.3 Calcular cumplimiento (POR FILAS)
      const compliance_in =
        adjusted > 0 ? Number(((real_in / adjusted) * 100).toFixed(2)) : 0;

      const compliance_global =
        expected > 0 ? Number(((real_total / expected) * 100).toFixed(2)) : 0;

      // 4.3.1 Flags (Opción 1)
      // met_in y met_global SOLO si hay base (>0) y supera threshold
      const met_in = adjusted > 0 && compliance_in >= threshold;
      const met_global = expected > 0 && compliance_global >= threshold;

      // 4.4 Insertar / actualizar progress
      await client.query(
        `
        INSERT INTO core.progress (
          user_id, period_year, period_month,
          real_in_count, real_out_count, real_total_count,
          expected_count, adjusted_count,
          compliance_in_percent, compliance_global_percent,
          met_in_district, met_global,
          created_at, updated_at
        )
        VALUES (
          $1,$2,$3,
          $4,$5,$6,
          $7,$8,
          $9,$10,
          $11,$12,
          NOW(), NOW()
        )
        ON CONFLICT (user_id, period_year, period_month)
        DO UPDATE SET
          real_in_count = EXCLUDED.real_in_count,
          real_out_count = EXCLUDED.real_out_count,
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

      // Si algún día quieres guardar cantserv en otra tabla o log:
      // console.log({ asesor_id, period_year, period_month, cantserv_in, cantserv_out });
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
      upserted
    };
  } catch (e) {
    await client.query("ROLLBACK");
    console.error("[PROMOTE_SIAPP_PROGRESS]", e);
    throw e;
  } finally {
    client.release();
  }
}

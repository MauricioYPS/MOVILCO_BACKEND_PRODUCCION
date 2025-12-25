// services/promote.siapp.service.js
import pool from '../config/database.js'
import { loadSettings } from './settings.service.js'

/**
 * promoteSiappFromFullSales
 * Calcula métricas IN / OUT / KPIs para cada asesor
 * usando el SIAPP FULL (siapp.full_sales)
 * y guarda resultados en core.progress.
 *
 * REGLA:
 *  - Match SIEMPRE por IDASESOR (fs.idasesor <-> core.users.document_id)
 *  - NO usar cedula_vendedor para nada de matching.
 *  - cantserv es VARCHAR => parse seguro a número.
 */
export async function promoteSiappFromFullSales({ period_year, period_month }) {
  const client = await pool.connect()

  // Parse robusto de cantserv (VARCHAR)
  const parseCantServ = (v) => {
    if (v === null || v === undefined) return 0
    const s = String(v).trim()
    if (!s) return 0

    // Normaliza coma decimal
    const normalized = s.replace(',', '.')
    const n = Number(normalized)
    if (Number.isFinite(n)) return n

    // Fallback: extraer primer número del string
    const m = normalized.match(/-?\d+(\.\d+)?/)
    return m ? Number(m[0]) : 0
  }

  // Normaliza keys (IDASESOR y document_id)
  const normId = (x) => (x === null || x === undefined ? '' : String(x).trim())

  try {
    await client.query('BEGIN')

    // 1. Leer configuración global
    const settings = await loadSettings(client)

    // 2. Obtener ventas reales desde siapp.full_sales
    // IMPORTANT: columnas reales en tu tabla -> idasesor, nombreasesor
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
    )

    // Agrupar por asesor (IDASESOR)
    const asesores = {}
    for (const s of sales) {
      const key = normId(s.id_asesor)
      if (!key) continue

      if (!asesores[key]) {
        asesores[key] = {
          id_asesor: key,
          nombre_asesor: s.nombre_asesor || null,
          ventas: [],
        }
      }
      asesores[key].ventas.push(s)
    }

    // 3. Obtener usuarios reales (match por document_id = IDASESOR)
    const { rows: users } = await client.query(
      `
      SELECT id, document_id AS id_asesor, district_claro, district
      FROM core.users
      WHERE document_id IS NOT NULL
      `
    )

    const userMap = {}
    for (const u of users) {
      const key = normId(u.id_asesor)
      if (!key) continue
      userMap[key] = u
    }

    // 4. Procesar cada asesor y hacer UPSERT en core.progress
    let upserted = 0
    let matchedUsers = 0

    for (const asesor_id of Object.keys(asesores)) {
      const data = asesores[asesor_id]
      const u = userMap[asesor_id]

      if (!u) continue // asesor no existe en usuarios (fuera nómina/presupuesto)

      matchedUsers++

      const ventas = data.ventas

      // 4.1 Calcular IN / OUT
      let real_in = 0
      let real_out = 0

      const d_user = (u.district_claro || u.district || '').trim().toUpperCase()

      for (const v of ventas) {
        const d_venta = (v.distrito_venta || '').trim().toUpperCase()
        const c = parseCantServ(v.cantserv)

        if (d_user && d_venta && d_venta === d_user) real_in += c
        else real_out += c
      }

      const real_total = real_in + real_out

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
      )

      let expected = 0
      let adjusted = 0
      if (umRows.length > 0) {
        expected = Number(umRows[0].presupuesto_mes || 0)
        adjusted = Number(umRows[0].prorrateo || expected)
      }

      // 4.3 Calcular cumplimiento
      const compliance_in =
        adjusted > 0 ? Number(((real_in / adjusted) * 100).toFixed(2)) : 0

      const compliance_global =
        expected > 0 ? Number(((real_total / expected) * 100).toFixed(2)) : 0

      const met_in = compliance_in >= Number(settings.min_compliance_in || 0)
      const met_global = compliance_global >= Number(settings.min_compliance_global || 0)

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
          met_global,
        ]
      )

      upserted++
    }

    await client.query('COMMIT')

    return {
      ok: true,
      period_year,
      period_month,
      total_sales_rows: sales.length,
      total_asesores_en_siapp: Object.keys(asesores).length,
      matched_users: matchedUsers,
      upserted
    }
  } catch (e) {
    await client.query('ROLLBACK')
    console.error('[PROMOTE_SIAPP_PROGRESS]', e)
    throw e
  } finally {
    client.release()
  }
}

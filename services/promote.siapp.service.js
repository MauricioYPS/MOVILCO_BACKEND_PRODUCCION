// services/promote.siapp.service.js
import pool from '../config/database.js'
import { loadSettings } from './settings.service.js'

/**
 * promoteSiappFromFullSales
 * Calcula métricas IN / OUT / KPIs para cada asesor
 * usando el SIAPP FULL (siapp.full_sales)
 * y guarda resultados en core.progress.
 */
export async function promoteSiappFromFullSales({ period_year, period_month }) {
  const client = await pool.connect()
  try {
    await client.query('BEGIN')

    // 1. Leer configuración global
    const settings = await loadSettings(client)

    // 2. Obtener ventas reales desde siapp.full_sales
    const { rows: sales } = await client.query(
      `
      SELECT
        fs.id_asesor,
        fs.nombre_asesor,
        fs.d_distrito AS distrito_venta,
        fs.cantserv
      FROM siapp.full_sales fs
      WHERE fs.period_year = $1
      AND fs.period_month = $2
      AND fs.id_asesor IS NOT NULL
      `,
      [period_year, period_month]
    )

    // Agrupar por asesor
    const asesores = {}
    for (const s of sales) {
      if (!asesores[s.id_asesor]) {
        asesores[s.id_asesor] = {
          id_asesor: s.id_asesor,
          nombre_asesor: s.nombre_asesor,
          ventas: [],
        }
      }
      asesores[s.id_asesor].ventas.push(s)
    }

    // 3. Obtener usuarios reales (para saber su distrito)
    const { rows: users } = await client.query(
      `
      SELECT id, document_id AS id_asesor, district_claro, district
      FROM core.users
      WHERE document_id IS NOT NULL
      `
    )

    const userMap = {}
    for (const u of users) userMap[u.id_asesor] = u

    // 4. Procesar cada asesor
    let inserted = 0
    for (const asesor_id of Object.keys(asesores)) {
      const data = asesores[asesor_id]
      const u = userMap[asesor_id]

      if (!u) continue // asesor no existe en usuarios

      const ventas = data.ventas

      // 4.1 Calcular IN / OUT
      let real_in = 0
      let real_out = 0

      for (const v of ventas) {
        const d_venta = (v.distrito_venta || '').trim().toUpperCase()
        const d_user = (u.district_claro || u.district || '').trim().toUpperCase()

        if (d_venta === d_user) real_in += v.cantserv || 0
        else real_out += v.cantserv || 0
      }

      const real_total = real_in + real_out

      // 4.2 Tomar información mensual desde user_monthly
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
        expected = umRows[0].presupuesto_mes || 0
        adjusted = umRows[0].prorrateo || expected
      }

      // 4.3 Calcular cumplimiento
      const compliance_in =
        adjusted > 0 ? Number(((real_in / adjusted) * 100).toFixed(2)) : 0

      const compliance_global =
        expected > 0 ? Number(((real_total / expected) * 100).toFixed(2)) : 0

      const met_in = compliance_in >= settings.min_compliance_in
      const met_global = compliance_global >= settings.min_compliance_global

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

      inserted++
    }

    await client.query('COMMIT')

    return {
      ok: true,
      inserted,
      total_asesores: Object.keys(asesores).length,
    }
  } catch (e) {
    await client.query('ROLLBACK')
    console.error('[PROMOTE_SIAPP_FULL_PROGRESS]', e)
    throw e
  } finally {
    client.release()
  }
}

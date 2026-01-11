// services/promote.presupuesto.service.js
import pool from "../config/database.js";
import { upsertBudgetsBatch } from "./budgets.service.js";

function onlyDigits(v) {
  const s = String(v ?? "").replace(/\D/g, "").trim();
  return s === "" ? null : s;
}

function parsePeriod(period) {
  const p = String(period || "").trim();
  const m = p.match(/^(\d{4})-(0[1-9]|1[0-2])$/);
  if (!m) return null;
  return { period: p };
}

function toIntNonNeg(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return 0;
  return Math.max(Math.trunc(n), 0);
}

/**
 * Devuelve una expresión SQL para obtener el nombre desde `raw` (jsonb),
 * intentando varias keys (incluye keys con espacios al final).
 */
function sqlNameFromRaw() {
  // Nota: ->> retorna text. NULLIF/TRIM evita strings vacíos.
  return `
    COALESCE(
      NULLIF(TRIM(s.raw->>'NOMBRE DE FUNCIONARIO'), ''),
      NULLIF(TRIM(s.raw->>'NOMBRE DE FUNCIONARIO '), ''),
      NULLIF(TRIM(s.raw->>'NOMBRE DE FUNCIONARIO  '), ''),
      NULLIF(TRIM(s.raw->>'NOMBRE FUNCIONARIO'), ''),
      NULLIF(TRIM(s.raw->>'NOMBRE_FUNCIONARIO'), ''),
      NULLIF(TRIM(s.raw->>'nombre_funcionario'), ''),
      NULLIF(TRIM(s.raw->>'NOMBRE'), ''),
      NULLIF(TRIM(s.raw->>'nombre'), ''),
      NULLIF(TRIM(s.raw->>'FUNCIONARIO'), ''),
      'SIN NOMBRE'
    )
  `;
}

export async function promotePresupuestoFromStaging({ period, batch_id, actor_user_id }) {
  const per = parsePeriod(period);
  if (!per) throw new Error("period inválido. Debe ser YYYY-MM (ej: 2026-01)");

  const client = await pool.connect();
  try {
    // 1) Elegir batch (si no llega, usar el más reciente)
    let batchToUse = batch_id;

    if (!batchToUse) {
      const q = await client.query(`
        SELECT batch_id
        FROM staging.archivo_presupuesto
        ORDER BY loaded_at DESC
        LIMIT 1
      `);
      batchToUse = q.rows[0]?.batch_id || null;
    }

    if (!batchToUse) {
      throw new Error("No hay datos en staging.archivo_presupuesto para promover (batch vacío)");
    }

    // 2) Traer filas del batch + resolver user_id en una sola query
    // - Normalizamos documento desde staging.cedula (puede traer puntos/espacios)
    // - Extraemos nombre desde raw (jsonb) para missing_users_sample
    const { rows } = await client.query(
      `
      SELECT
        NULLIF(regexp_replace(COALESCE(s.cedula::text, ''), '\\D', '', 'g'), '') AS document_id,
        ${sqlNameFromRaw()} AS nombre,
        s.presupuesto AS presupuesto,
        u.id AS user_id
      FROM staging.archivo_presupuesto s
      LEFT JOIN core.users u
        ON u.document_id = regexp_replace(COALESCE(s.cedula::text, ''), '\\D', '', 'g')
      WHERE s.batch_id = $1
      ORDER BY s.id ASC
      `,
      [batchToUse]
    );

    // 3) Armar items
    const total = rows.length;
    let skippedNoDocument = 0;
    let skippedMissingUser = 0;

    const missing_users_sample = [];
    const items = [];

    for (const r of rows) {
      const document_id = onlyDigits(r.document_id);
      if (!document_id) {
        skippedNoDocument++;
        continue;
      }

      const amount = toIntNonNeg(r.presupuesto);

      const userId = r.user_id != null ? Number(r.user_id) : null;
      if (!userId || !Number.isFinite(userId) || userId <= 0) {
        skippedMissingUser++;
        if (missing_users_sample.length < 100) {
          missing_users_sample.push({
            document_id,
            nombre: r.nombre || "SIN NOMBRE",
            presupuesto: amount,
            motivo: "NO_EXISTE_EN_CORE_USERS",
          });
        }
        continue;
      }

      items.push({
        user_id: userId,
        budget_amount: amount,
        status: "ACTIVE",
        unit: "CONNECTIONS",
      });
    }

    if (items.length === 0) {
      return {
        ok: true,
        period: per.period,
        batch_id: batchToUse,
        total_rows: total,
        items_valid: 0,
        skippedNoDocument,
        skippedMissingUser,
        missing_users_sample,
        note: "No hubo items válidos para upsert.",
      };
    }

    // 4) Pipeline oficial de budgets (sync legacy + recalc monthly + recalc progress)
    const upsertResult = await upsertBudgetsBatch({
      period: per.period,
      scope: "MONTHLY",
      items,
      actor_user_id,
    });

    return {
      ok: true,
      period: per.period,
      batch_id: batchToUse,
      total_rows: total,
      items_valid: items.length,
      skippedNoDocument,
      skippedMissingUser,
      missing_users_sample,
      budgets_result: upsertResult,
    };
  } finally {
    client.release();
  }
}

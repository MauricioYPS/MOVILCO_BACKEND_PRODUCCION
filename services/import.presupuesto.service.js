// services/import.presupuesto.service.js
import pool from "../config/database.js";
import * as XLSX from "xlsx";

function onlyDigits(v) {
  const s = String(v ?? "").replace(/\D/g, "").trim();
  return s === "" ? null : s;
}

function toIntOrNull(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim().replace(",", ".");
  if (!s) return null;
  const n = Number(s);
  if (!Number.isFinite(n)) return null;
  return Math.trunc(n);
}

function normalizeHeader(h) {
  return String(h ?? "")
    .trim()
    .toUpperCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

export async function importPresupuestoToStaging({ buffer, originalName }) {
  const wb = XLSX.read(buffer, { type: "buffer" });
  const sheetName = wb.SheetNames?.[0];
  if (!sheetName) throw new Error("El archivo no tiene hojas");

  const ws = wb.Sheets[sheetName];
  const json = XLSX.utils.sheet_to_json(ws, { defval: null });

  if (!Array.isArray(json) || json.length === 0) {
    return {
      batch_id: null,
      inserted: 0,
      total_rows: 0,
      columns: [],
      skippedNoDocument: 0,
      skippedNoPresupuesto: 0,
      sample_skipped: [],
    };
  }

  // Detectar columnas (headers)
  const first = json[0] || {};
  const headers = Object.keys(first);
  const normHeaders = headers.map(normalizeHeader);

  // Map requerido
  // CEDULA puede venir como "CEDULA", "CÉDULA", "CEDULA " etc.
  // PRESUPUESTO viene "PRESUPUESTO"
  const findKey = (targetNorm) => {
    const idx = normHeaders.findIndex((h) => h === targetNorm);
    return idx >= 0 ? headers[idx] : null;
  };

  const keyCedula = findKey("CEDULA") || findKey("CÉDULA".normalize("NFD").replace(/[\u0300-\u036f]/g, "")) || null;
  const keyPresupuesto = findKey("PRESUPUESTO");

  if (!keyCedula) throw new Error("No se encontró columna CEDULA en el Excel");
  if (!keyPresupuesto) throw new Error("No se encontró columna PRESUPUESTO en el Excel");

  const batch_id = `${Date.now()}`; // estable y simple
  const client = await pool.connect();

  let inserted = 0;
  let skippedNoDocument = 0;
  let skippedNoPresupuesto = 0;
  const sample_skipped = [];

  try {
    await client.query("BEGIN");

    for (const row of json) {
      const cedula = onlyDigits(row[keyCedula]);
      const presupuesto = toIntOrNull(row[keyPresupuesto]);

      if (!cedula) {
        skippedNoDocument++;
        if (sample_skipped.length < 50) {
          sample_skipped.push({ motivo: "NO_CEDULA", row });
        }
        continue;
      }

      if (presupuesto === null) {
        skippedNoPresupuesto++;
        if (sample_skipped.length < 50) {
          sample_skipped.push({ motivo: "NO_PRESUPUESTO", cedula, row });
        }
        continue;
      }

      await client.query(
        `
        INSERT INTO staging.archivo_presupuesto
          (batch_id, source_filename, cedula, presupuesto, raw)
        VALUES
          ($1, $2, $3, $4, $5::jsonb)
        `,
        [batch_id, originalName || null, cedula, Math.max(presupuesto, 0), JSON.stringify(row)]
      );

      inserted++;
    }

    await client.query("COMMIT");

    return {
      batch_id,
      inserted,
      total_rows: json.length,
      columns: headers,
      skippedNoDocument,
      skippedNoPresupuesto,
      sample_skipped,
    };
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

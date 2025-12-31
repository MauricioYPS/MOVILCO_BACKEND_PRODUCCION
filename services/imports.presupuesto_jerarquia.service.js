// services/imports.presupuesto_jerarquia.service.js
import ExcelJS from "exceljs";
import pool from "../config/database.js";

const SHEET_NAME = "Presupuesto Jerarquia"; // nombre exacto de la hoja

// =====================================================
// Utils
// =====================================================
function normalize(v) {
  if (v == null) return "";
  return String(v)
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeUpper(v) {
  return normalize(v).toUpperCase();
}

function cellToText(v) {
  if (v == null) return "";
  if (typeof v === "object") {
    if (Array.isArray(v.richText)) return v.richText.map(t => t.text || "").join("");
    if (typeof v.text === "string") return v.text;
    if (v.result != null) return String(v.result);
  }
  return String(v);
}

/**
 * Junta textos del encabezado para una misma columna en N filas (span)
 * Sirve cuando Excel trae encabezados “combinados” en 2-3 filas.
 */
function collectHeaderTexts(ws, headerRow, span = 3) {
  const maxCol = ws.columnCount || (ws.getRow(headerRow)?.values?.length ?? 0);
  const out = Array(maxCol + 1).fill("");

  for (let c = 1; c <= maxCol; c++) {
    const parts = [];
    for (let r = headerRow; r < headerRow + span && r <= ws.rowCount; r++) {
      const v = ws.getRow(r)?.getCell(c)?.value;
      const t = normalizeUpper(cellToText(v));
      if (t) parts.push(t);
    }
    out[c] = parts.join(" ").replace(/\s+/g, " ").trim();
  }

  return out;
}

function toDate(v) {
  if (!v) return null;

  if (v instanceof Date) return v.toISOString().slice(0, 10);

  if (typeof v === "number") {
    const epoch = new Date(Date.UTC(1899, 11, 30));
    const date = new Date(epoch.getTime() + v * 86400000);
    return date.toISOString().slice(0, 10);
  }

  const s = normalize(v);
  const m1 = s.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (m1) return `${m1[3]}-${m1[2]}-${m1[1]}`;

  const m2 = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (m2) return s;

  return null;
}

function toNumber(v) {
  if (v == null || v === "") return null;
  if (typeof v === "number") return v;
  const n = Number(String(v).replace(/[^0-9.-]/g, ""));
  return Number.isFinite(n) ? n : null;
}

function rowHasAnyValue(rec) {
  return Object.values(rec).some(v => v !== null && v !== "");
}

// =====================================================
// Detectar fila de encabezados
// =====================================================
function findHeaderRow(ws) {
  for (let r = 1; r <= 30; r++) {
    const row = ws.getRow(r);
    const vals = (row.values || []).map(v => normalizeUpper(cellToText(v)));
    const hasCedula = vals.some(t => t.includes("CEDULA"));
    const hasNombre = vals.some(t => t.includes("NOMBRE"));
    if (hasCedula && hasNombre) return r;
  }
  return null;
}

// =====================================================
// Mapeo columnas (robusto multi-fila)
// =====================================================
function buildColumnIndex(ws, headerRow) {
  const headers = collectHeaderTexts(ws, headerRow, 3);

  const findAny = (keywords) => {
    const norms = keywords.map(k => normalizeUpper(k));
    for (let c = 1; c < headers.length; c++) {
      const text = headers[c];
      if (!text) continue;
      for (const k of norms) {
        if (text.includes(k)) return c;
      }
    }
    return -1;
  };

  // Importante: agregamos variantes posibles de “CONTRATADO”
  const wanted = {
    jerarquia: ["JERARQUIA"],
    cargo: ["CARGO"],
    cedula: ["CEDULA", "DOCUMENTO", "CC"],
    nombre: ["NOMBRE", "FUNCIONARIO"],
    contratado: ["CONTRATADO", "CONTRATA DO", "ESTADO", "VIGENTE", "ACTIVO", "RETIRADO"],
    distrito: ["DISTRITO"],
    regional: ["REGIONAL"],
    fecha_inicio: ["FECHA INICIO"],
    fecha_fin: ["FECHA FIN"],
    presupuesto: ["PRESUPUESTO"],
    ejecutado: ["EJECUTADO"],
    cierre: ["CIERRE"],
    capacidad: ["CAPACIDAD"],
    telefono: ["TELEFONO", "TEL"],
    correo: ["CORREO", "EMAIL"]
  };

  return {
    jerarquia: findAny(wanted.jerarquia),
    cargo: findAny(wanted.cargo),
    cedula: findAny(wanted.cedula),
    nombre: findAny(wanted.nombre),
    contratado: findAny(wanted.contratado),
    distrito: findAny(wanted.distrito),
    regional: findAny(wanted.regional),
    fecha_inicio: findAny(wanted.fecha_inicio),
    fecha_fin: findAny(wanted.fecha_fin),
    presupuesto: findAny(wanted.presupuesto),
    ejecutado: findAny(wanted.ejecutado),
    cierre: findAny(wanted.cierre),
    capacidad: findAny(wanted.capacidad),
    telefono: findAny(wanted.telefono),
    correo: findAny(wanted.correo),
  };
}

// =====================================================
// IMPORTADOR PRINCIPAL
// =====================================================
export async function importPresupuestoJerarquia(buffer) {
  console.log("[IMPORT PJ] Cargando Excel...");

  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(buffer);

  const ws = wb.getWorksheet(SHEET_NAME);
  if (!ws) throw new Error(`No se encontró la hoja "${SHEET_NAME}"`);

  const headerRow = findHeaderRow(ws);
  if (!headerRow) throw new Error("No se encontró fila de encabezados");

  const idx = buildColumnIndex(ws, headerRow);

  // Cedula y nombre deben existir
  if (idx.cedula < 0 || idx.nombre < 0) {
    throw new Error("Encabezados insuficientes: no se encontró CEDULA y/o NOMBRE");
  }

  const rows = [];
  for (let r = headerRow + 1; r <= ws.rowCount; r++) {
    const row = ws.getRow(r);

    const rec = {
      jerarquia_raw: idx.jerarquia > 0 ? normalize(row.getCell(idx.jerarquia).value) : null,
      cargo_raw: idx.cargo > 0 ? normalize(row.getCell(idx.cargo).value) : null,
      cedula: idx.cedula > 0 ? normalize(row.getCell(idx.cedula).value).replace(/\D/g, "") : null,
      nombre_raw: idx.nombre > 0 ? normalize(row.getCell(idx.nombre).value) : null,

      // ✅ FIX: contratado_raw ahora sí se llena si existe en el Excel
      contratado_raw: idx.contratado > 0 ? normalize(row.getCell(idx.contratado).value) : null,

      distrito_raw: idx.distrito > 0 ? normalize(row.getCell(idx.distrito).value) : null,
      regional_raw: idx.regional > 0 ? normalize(row.getCell(idx.regional).value) : null,
      fecha_inicio: idx.fecha_inicio > 0 ? toDate(row.getCell(idx.fecha_inicio).value) : null,
      fecha_fin: idx.fecha_fin > 0 ? toDate(row.getCell(idx.fecha_fin).value) : null,
      presupuesto_raw: idx.presupuesto > 0 ? toNumber(row.getCell(idx.presupuesto).value) : null,
      ejecutado_raw: idx.ejecutado > 0 ? toNumber(row.getCell(idx.ejecutado).value) : null,
      cierre_raw: idx.cierre > 0 ? toNumber(row.getCell(idx.cierre).value) : null,
      capacidad_raw: idx.capacidad > 0 ? toNumber(row.getCell(idx.capacidad).value) : null,
      telefono_raw: idx.telefono > 0 ? normalize(row.getCell(idx.telefono).value) : null,
      correo_raw: idx.correo > 0 ? normalize(row.getCell(idx.correo).value) : null,
    };

    if (rowHasAnyValue(rec)) rows.push(rec);
  }

  console.log(`[IMPORT PJ] Filas detectadas: ${rows.length}`);

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // OJO: Mantienes tu truncate de PJ
    await client.query("TRUNCATE TABLE core.presupuesto_jerarquia RESTART IDENTITY");

    const insertCols = `
      jerarquia_raw, cargo_raw, cedula, nombre_raw, contratado_raw,
      distrito_raw, regional_raw, fecha_inicio, fecha_fin,
      presupuesto_raw, ejecutado_raw, cierre_raw,
      capacidad_raw, telefono_raw, correo_raw
    `;

    const insertSQL = `
      INSERT INTO core.presupuesto_jerarquia (${insertCols})
      VALUES
      ${rows.map((_, i) => {
        const b = i * 15;
        return `(
          $${b + 1}, $${b + 2}, $${b + 3}, $${b + 4}, $${b + 5},
          $${b + 6}, $${b + 7}, $${b + 8}, $${b + 9},
          $${b + 10}, $${b + 11}, $${b + 12},
          $${b + 13}, $${b + 14}, $${b + 15}
        )`;
      }).join(", ")}
    `;

    const values = rows.flatMap(r => [
      r.jerarquia_raw,
      r.cargo_raw,
      r.cedula,
      r.nombre_raw,
      r.contratado_raw,
      r.distrito_raw,
      r.regional_raw,
      r.fecha_inicio,
      r.fecha_fin,
      r.presupuesto_raw,
      r.ejecutado_raw,
      r.cierre_raw,
      r.capacidad_raw,
      r.telefono_raw,
      r.correo_raw,
    ]);

    if (rows.length > 0) await client.query(insertSQL, values);

    // ============================================================
    // ✅ REGLA NEGOCIO (tuya):
    // “Se importa una vez y todos quedan activos”.
    // Reactiva todos los usuarios que vienen en PJ.
    // ============================================================
    const { rowCount: reactivated } = await client.query(`
      UPDATE core.users u
      SET active = true, updated_at = now()
      FROM core.presupuesto_jerarquia pj
      WHERE pj.cedula = u.document_id
        AND u.active IS DISTINCT FROM true
    `);

    await client.query("COMMIT");

    console.log("[IMPORT PJ] Importación finalizada OK");
    return { inserted: rows.length, users_reactivated: reactivated };

  } catch (err) {
    await client.query("ROLLBACK");
    console.error("[IMPORT PJ] Error:", err);
    throw err;
  } finally {
    client.release();
  }
}

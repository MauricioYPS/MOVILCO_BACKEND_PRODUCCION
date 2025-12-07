import ExcelJS from "exceljs";
import pool from "../config/database.js";

const SHEET_NAME = "Presupuesto Jerarquia"; // nombre exacto

// -------------------------------
// HELPERS
// -------------------------------
function normalize(v) {
  if (v == null) return "";
  return String(v)
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .trim();
}

function normalizeUpper(v) {
  return normalize(v).toUpperCase();
}

function toDate(v) {
  if (!v) return null;

  if (v instanceof Date) {
    return v.toISOString().slice(0, 10);
  }

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

// -------------------------------
// DETECTAR FILA DE ENCABEZADOS
// -------------------------------
function findHeaderRow(ws) {
  for (let r = 1; r <= 20; r++) {
    const row = ws.getRow(r);
    const vals = row.values.map(v => normalizeUpper(v));

    if (vals.includes("CEDULA") && vals.some(t => t.includes("NOMBRE"))) {
      return r;
    }
  }
  return null;
}

// -------------------------------
// MAPEAR COLUMNAS
// -------------------------------
function buildColumnIndex(ws, headerRow) {
  const row = ws.getRow(headerRow);
  const cols = row.values.map(v => (v ? normalize(v) : ""));

  const findAny = (keywords) => {
    const norms = keywords.map(k => normalize(k));

    for (let c = 1; c < cols.length; c++) {
      const text = cols[c];
      if (!text) continue;
      for (const k of norms) {
        if (text.includes(k)) return c;
      }
    }
    return -1;
  };

  const wanted = {
    jerarquia: ["JERARQUIA"],
    cargo: ["CARGO"],
    cedula: ["CEDULA", "DOCUMENTO", "CC"],
    nombre: ["NOMBRE", "FUNCIONARIO"],
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

function rowHasAnyValue(rec) {
  return Object.values(rec).some(v => v !== null && v !== "");
}

// ======================================================
// IMPORTADOR PRINCIPAL
// ======================================================
export async function importPresupuestoJerarquia(buffer) {
  console.log("[IMPORT PJ] Cargando Excel...");

  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(buffer);

  const ws = wb.getWorksheet(SHEET_NAME);
  if (!ws) throw new Error(`No se encontró la hoja "${SHEET_NAME}"`);

  const headerRow = findHeaderRow(ws);
  if (!headerRow) throw new Error("No se encontró fila de encabezados");

  const idx = buildColumnIndex(ws, headerRow);

  const rows = [];

  for (let r = headerRow + 1; r <= ws.rowCount; r++) {
    const row = ws.getRow(r);

    const rec = {
      jerarquia_raw: idx.jerarquia > 0 ? normalize(row.getCell(idx.jerarquia).value) : null,
      cargo_raw: idx.cargo > 0 ? normalize(row.getCell(idx.cargo).value) : null,
      cedula: idx.cedula > 0 ? normalize(row.getCell(idx.cedula).value).replace(/\D/g, "") : null,
      nombre_raw: idx.nombre > 0 ? normalize(row.getCell(idx.nombre).value) : null,
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

    await client.query("TRUNCATE TABLE core.presupuesto_jerarquia RESTART IDENTITY");

    const insertCols = `
      jerarquia_raw, cargo_raw, cedula, nombre_raw, distrito_raw, regional_raw,
      fecha_inicio, fecha_fin, presupuesto_raw, ejecutado_raw, cierre_raw,
      capacidad_raw, telefono_raw, correo_raw
    `;

    const insertSQL = `
      INSERT INTO core.presupuesto_jerarquia
      (${insertCols})
      VALUES
      ${rows.map((_, i) => {
        const b = i * 14;
        return `(
          $${b+1}, $${b+2}, $${b+3}, $${b+4}, $${b+5}, $${b+6},
          $${b+7}, $${b+8}, $${b+9}, $${b+10}, $${b+11},
          $${b+12}, $${b+13}, $${b+14}
        )`;
      }).join(", ")}
    `;

    const values = rows.flatMap(r => [
      r.jerarquia_raw,
      r.cargo_raw,
      r.cedula,
      r.nombre_raw,
      r.distrito_raw,
      r.regional_raw,
      r.fecha_inicio,
      r.fecha_fin,
      r.presupuesto_raw,
      r.ejecutado_raw,
      r.cierre_raw,
      r.capacidad_raw,
      r.telefono_raw,
      r.correo_raw
    ]);

    if (rows.length > 0) await client.query(insertSQL, values);

    await client.query("COMMIT");
    console.log("[IMPORT PJ] Importación finalizada OK");

    return { ok: true, inserted: rows.length };

  } catch (err) {
    await client.query("ROLLBACK");
    console.error("[IMPORT PJ] Error:", err);
    throw err;
  } finally {
    client.release();
  }
}

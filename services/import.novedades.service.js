// services/import.novedades.service.js
import ExcelJS from "exceljs";
import pool from "../config/database.js";

const SHEET_NAME = "ACT";
const TABLE = "staging.archivo_novedades";

// Columnas EXACTAS (pero toleramos variaciones por normalize)
const WANTED = {
  cedula: ["CEDULA", "CC", "DOCUMENTO"],
  nombre: ["NOMBRE DE FUNCIONARIO", "NOMBRE", "FUNCIONARIO"],
  novedades: ["NOVEDADES DE AUSENTISMO", "NOVEDADES AUSENTISMO", "AUSENTISMO"]
};

function normalize(s) {
  if (s == null) return "";
  return String(s)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
}

function cellToText(v) {
  if (v == null) return "";
  if (typeof v === "object") {
    if (Array.isArray(v.richText)) return v.richText.map((t) => t.text || "").join("");
    if (typeof v.text === "string") return v.text;
    if (v.result != null) return String(v.result);
  }
  return String(v);
}

function onlyDigits(v) {
  const s = String(v ?? "").replace(/\D/g, "").trim();
  return s === "" ? null : s;
}

function rowIsEmpty(arr) {
  if (!arr) return true;
  return arr.every((v) => v == null || String(v).trim() === "");
}

function collectHeaderTexts(ws, headerRow, span = 3) {
  const maxCol = ws.columnCount || (ws.getRow(headerRow)?.values?.length ?? 0);
  const out = Array(maxCol + 1).fill("");
  for (let c = 1; c <= maxCol; c++) {
    const parts = [];
    for (let r = headerRow; r < headerRow + span && r <= ws.rowCount; r++) {
      const v = ws.getRow(r)?.getCell(c)?.value;
      const t = normalize(cellToText(v));
      if (t) parts.push(t);
    }
    out[c] = parts.join(" ").replace(/\s+/g, " ").trim();
  }
  return out;
}

function findHeaderRow(ws) {
  const maxScan = Math.min(30, ws.rowCount || 0);
  for (let r = 1; r <= maxScan; r++) {
    const texts = (ws.getRow(r).values || []).map((v) => normalize(v ?? ""));
    const hasCedula = texts.some((t) => t.includes("CEDULA"));
    const hasNombre = texts.some((t) => t.includes("NOMBRE"));
    const hasNov = texts.some((t) => t.includes("AUSENTISMO") || t.includes("NOVEDADES"));
    if (hasCedula && hasNombre && hasNov) return r;
  }
  return null;
}

function buildIndex(ws, headerRow) {
  const cols = collectHeaderTexts(ws, headerRow, 3);

  const findAny = (keywords) => {
    for (let c = 1; c < cols.length; c++) {
      const head = cols[c] || "";
      for (const k of keywords) {
        if (head.includes(normalize(k))) return c;
      }
    }
    return -1;
  };

  return {
    cedula: findAny(WANTED.cedula),
    nombre: findAny(WANTED.nombre),
    novedades: findAny(WANTED.novedades)
  };
}

/**
 * Importa novedades desde Excel a staging.archivo_novedades.
 * Soporta:
 *  - { buffer } (multer.memoryStorage)
 *  - { filePath } (compatibilidad legacy)
 */
export async function importNovedadesToStaging({
  buffer = null,
  filePath = null,
  sourceFilename = null
} = {}) {
  if (!buffer && !filePath) {
    throw new Error("Debes enviar buffer o filePath para leer el Excel.");
  }

  const wb = new ExcelJS.Workbook();

  // ✅ NUEVO: soportar memoria
  if (buffer) {
    await wb.xlsx.load(buffer);
  } else {
    await wb.xlsx.readFile(filePath);
  }

  const ws = wb.getWorksheet(SHEET_NAME) || wb.worksheets?.[0];
  if (!ws) throw new Error(`No se encontró la hoja "${SHEET_NAME}"`);

  const headerRow = findHeaderRow(ws);
  if (!headerRow) {
    throw new Error(
      "No se detectó la fila de encabezados (debe contener CEDULA, NOMBRE y NOVEDADES DE AUSENTISMO)."
    );
  }

  const idx = buildIndex(ws, headerRow);
  if (idx.cedula < 0 || idx.nombre < 0 || idx.novedades < 0) {
    throw new Error(
      "Encabezados insuficientes en ACT (se requiere CEDULA, NOMBRE DE FUNCIONARIO y NOVEDADES DE AUSENTISMO)."
    );
  }

  const batch_id = String(Date.now());

  let total_rows = 0;
  let inserted = 0;
  let skippedNoDocument = 0;
  let skippedEmptyNovedades = 0;

  const rowsToInsert = [];

  for (let r = headerRow + 1; r <= ws.rowCount; r++) {
    const row = ws.getRow(r);
    const rawArr = (row.values || []).slice(1);
    if (rowIsEmpty(rawArr)) continue;

    total_rows++;

    const cedulaRaw = cellToText(row.getCell(idx.cedula).value);
    const document_id = onlyDigits(cedulaRaw); // <- cédula limpia según tu nota, pero lo dejamos robusto
    if (!document_id) {
      skippedNoDocument++;
      continue;
    }

    const nombre = String(cellToText(row.getCell(idx.nombre).value) ?? "").trim() || null;
    const novedades_text =
      String(cellToText(row.getCell(idx.novedades).value) ?? "").trim() || null;

    if (!novedades_text) {
      skippedEmptyNovedades++;
      continue;
    }

    // raw opcional para auditoría/depuración
    const raw = {
      row: r,
      CEDULA: cedulaRaw,
      NOMBRE: nombre,
      NOVEDADES_DE_AUSENTISMO: novedades_text
    };

    rowsToInsert.push([
      batch_id,
      sourceFilename,
      document_id,
      nombre,
      novedades_text,
      raw // ✅ jsonb real (no string)
    ]);
  }

  if (rowsToInsert.length === 0) {
    return {
      dataset: "novedades",
      table: TABLE,
      batch_id,
      inserted: 0,
      total_rows,
      skippedNoDocument,
      skippedEmptyNovedades,
      note: "No hubo filas válidas para staging (revisa CEDULA y NOVEDADES DE AUSENTISMO)."
    };
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // Insert masivo por chunks
    const columns = ["batch_id", "source_filename", "cedula", "nombre", "novedades_text", "raw"];
    const colsSql = columns.map((c) => `"${c}"`).join(",");
    const MAX_PARAMS = 60000;
    const chunkSize = Math.max(1, Math.floor(MAX_PARAMS / columns.length));

    for (let offset = 0; offset < rowsToInsert.length; offset += chunkSize) {
      const chunk = rowsToInsert.slice(offset, offset + chunkSize);

      const placeholders = chunk
        .map((_, i) => {
          const base = i * columns.length;
          const slots = columns.map((__, j) => `$${base + j + 1}`);
          return `(${slots.join(",")})`;
        })
        .join(",");

      const values = [];
      for (const rr of chunk) for (const v of rr) values.push(v ?? null);

      const sql = `INSERT INTO ${TABLE} (${colsSql}) VALUES ${placeholders}`;
      await client.query(sql, values);
      inserted += chunk.length;
    }

    await client.query("COMMIT");

    return {
      dataset: "novedades",
      table: TABLE,
      batch_id,
      inserted,
      total_rows,
      skippedNoDocument,
      skippedEmptyNovedades,
      columns: ["CEDULA", "NOMBRE DE FUNCIONARIO", "NOVEDADES DE AUSENTISMO"]
    };
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

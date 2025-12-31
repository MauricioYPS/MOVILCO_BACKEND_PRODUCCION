// services/imports.nomina.service.js
import ExcelJS from "exceljs";
import pool from "../config/database.js";

// --- helpers de normalización ---
function normalize(s) {
  if (s == null) return "";
  return String(s)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "") // sin acentos
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
}

function toDate(v) {
  if (v == null || v === "") return null;
  if (v instanceof Date) return v.toISOString().slice(0, 10);

  // Excel serial
  if (typeof v === "number") {
    const epoch = new Date(Date.UTC(1899, 11, 30));
    const date = new Date(epoch.getTime() + v * 24 * 60 * 60 * 1000);
    return date.toISOString().slice(0, 10);
  }

  const t = normalize(v);

  // DD/MM/YYYY o DD-MM-YYYY
  const m1 = t.match(/^(\d{2})[\/\-](\d{2})[\/\-](\d{4})$/);
  if (m1) {
    const [, dd, mm, yyyy] = m1;
    return `${yyyy}-${mm}-${dd}`;
  }

  // YYYY/MM/DD o YYYY-MM-DD
  const m2 = t.match(/^(\d{4})[\/\-](\d{2})[\/\-](\d{2})$/);
  if (m2) return `${m2[1]}-${m2[2]}-${m2[3]}`;

  return null;
}

function toNumber(v) {
  if (v == null || v === "") return null;
  if (typeof v === "number") return v;
  const t = String(v).replace(/[^0-9.\-]/g, "");
  const n = Number(t);
  return Number.isFinite(n) ? n : null;
}

// --- mapeo flexible por encabezados ---
const wanted = {
  cedula: ["CEDULA", "CC", "DOCUMENTO"],
  nombre: ["NOMBRE DE FUNCIONARIO", "NOMBRE FUNCIONARIO", "FUNCIONARIO", "NOMBRE"],
  contratado: ["CONTRATADO", "ESTADO", "ACTIVO", "RETIRADO"],
  distrito: ["DISTRITO"],
  distrito_claro: ["DISTRITO CLARO", "DISTRITO DECLARO", "DISTRITO DECLARO*"],
  fecha_inicio: ["FECHA INICIO CONTRATO", "INICIO CONTRATO"],
  fecha_fin: ["FECHA FIN CONTRATO", "FIN CONTRATO"],
  novedad: ["NOVEDAD", "NOVEDADES"],
  presupuesto_mes: ["PRESUPUESTO MES", "PRESUPUESTO"],
  dias_laborados: ["DIAS LABORADOS", "DIAS LABORADOS AL 31", "DIAS LABORADOS AL 31 MES"],
  prorrateo: ["PRORRATEO", "PRORRATEO SEGUN NOVEDADES"],
  estado_envio: ["ESTADO ENVIO PRESUPUESTO", "ESTADO ENVIO"]
};

function findHeaderRow(ws) {
  // escanea primeras 40 filas buscando CEDULA y NOMBRE
  for (let r = 1; r <= Math.min(40, ws.rowCount); r++) {
    const row = ws.getRow(r);
    const values = (row.values || []).map((v) => normalize(v));
    const hasCedula = values.some((t) => t.includes("CEDULA"));
    const hasNombre = values.some((t) => t.includes("NOMBRE"));
    if (hasCedula && hasNombre) return r;
  }
  return null;
}

function buildColumnIndex(ws, headerRow) {
  const row = ws.getRow(headerRow);
  const cols = (row.values || []).map((v) => normalize(v));

  const findAny = (keywords) => {
    for (let c = 1; c < cols.length; c++) {
      const cellText = cols[c];
      for (const k of keywords) {
        if (cellText.includes(normalize(k))) return c;
      }
    }
    return -1;
  };

  return {
    cedula: findAny(wanted.cedula),
    nombre: findAny(wanted.nombre),
    contratado: findAny(wanted.contratado),
    distrito: findAny(wanted.distrito),
    distrito_claro: findAny(wanted.distrito_claro),
    fecha_inicio: findAny(wanted.fecha_inicio),
    fecha_fin: findAny(wanted.fecha_fin),
    novedad: findAny(wanted.novedad),
    presupuesto: findAny(wanted.presupuesto_mes),
    dias_lab: findAny(wanted.dias_laborados),
    prorrateo: findAny(wanted.prorrateo),
    estado_envio: findAny(wanted.estado_envio)
  };
}

function rowHasAnyValue(r) {
  return Object.values(r).some((v) => v != null && v !== "");
}

// Encuentra automáticamente la hoja correcta (sin depender del nombre)
function findWorksheetWithHeaders(wb) {
  for (const ws of wb.worksheets) {
    const headerRow = findHeaderRow(ws);
    if (!headerRow) continue;

    const idx = buildColumnIndex(ws, headerRow);
    if (idx.cedula > 0 && idx.nombre > 0) {
      return { ws, headerRow, idx };
    }
  }
  return null;
}

export async function importNominaFromExcel(buffer) {
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(buffer);

  const found = findWorksheetWithHeaders(wb);
  if (!found) {
    throw new Error(
      'No se encontró ninguna hoja con encabezados válidos (CEDULA y NOMBRE). Revisa el Excel.'
    );
  }

  const { ws, headerRow, idx } = found;

  // Recorrer filas
  const rows = [];
  for (let r = headerRow + 1; r <= ws.rowCount; r++) {
    const row = ws.getRow(r);

    // Saltar filas vacías
    const hasAny = (row.values || []).slice(1).some((v) => v != null && String(v).trim() !== "");
    if (!hasAny) continue;

    const rec = {
      cedula: idx.cedula > 0 ? normalize(row.getCell(idx.cedula).value).replace(/\D/g, "") || null : null,
      nombre_funcionario: idx.nombre > 0 ? String(row.getCell(idx.nombre).value || "").trim() || null : null,
      contratado: idx.contratado > 0 ? normalize(row.getCell(idx.contratado).value) || null : null,
      distrito: idx.distrito > 0 ? String(row.getCell(idx.distrito).value || "").trim() || null : null,
      distrito_claro: idx.distrito_claro > 0 ? String(row.getCell(idx.distrito_claro).value || "").trim() || null : null,
      fecha_inicio_contrato: idx.fecha_inicio > 0 ? toDate(row.getCell(idx.fecha_inicio).value) : null,
      fecha_fin_contrato: idx.fecha_fin > 0 ? toDate(row.getCell(idx.fecha_fin).value) : null,
      novedad: idx.novedad > 0 ? String(row.getCell(idx.novedad).value || "").trim() || null : null,
      presupuesto_mes: idx.presupuesto > 0 ? toNumber(row.getCell(idx.presupuesto).value) : null,
      dias_laborados: idx.dias_lab > 0 ? toNumber(row.getCell(idx.dias_lab).value) : null,
      prorrateo: idx.prorrateo > 0 ? toNumber(row.getCell(idx.prorrateo).value) : null,
      estado_envio_presupuesto: idx.estado_envio > 0 ? String(row.getCell(idx.estado_envio).value || "").trim() || null : null
    };

    if (rowHasAnyValue(rec)) rows.push(rec);
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("TRUNCATE TABLE staging.archivo_nomina");

    // Insert por lotes para no reventar límite de parámetros
    const BATCH = 1000;

    for (let i = 0; i < rows.length; i += BATCH) {
      const chunk = rows.slice(i, i + BATCH);

      const text = `
        INSERT INTO staging.archivo_nomina
        (cedula, nombre_funcionario, contratado, distrito, distrito_claro,
         fecha_inicio_contrato, fecha_fin_contrato, novedad, presupuesto_mes,
         dias_laborados, prorrateo, estado_envio_presupuesto, loaded_at)
        VALUES
        ${chunk
          .map((_, j) => {
            const base = j * 13;
            return `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},$${base + 6},$${base + 7},$${base + 8},$${base + 9},$${base + 10},$${base + 11},$${base + 12},$${base + 13})`;
          })
          .join(",")}
      `;

      const values = chunk.flatMap((r) => [
        r.cedula,
        r.nombre_funcionario,
        r.contratado,
        r.distrito,
        r.distrito_claro,
        r.fecha_inicio_contrato,
        r.fecha_fin_contrato,
        r.novedad,
        r.presupuesto_mes,
        r.dias_laborados,
        r.prorrateo,
        r.estado_envio_presupuesto,
        new Date() // loaded_at
      ]);

      await client.query(text, values);
    }

    await client.query("COMMIT");

    return {
      inserted: rows.length,
      sheet: ws.name,
      headerRow,
      columns: idx
    };
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

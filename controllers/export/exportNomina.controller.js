// controllers/export/nomina/exportNomina.controller.js
import ExcelJS from "exceljs";
import pool from "../../config/database.js";

// ======================================================
// Helpers
// ======================================================
function pad2(n) {
  return String(n).padStart(2, "0");
}

function toCODate(v) {
  if (!v) return null;
  const d = v instanceof Date ? v : new Date(v);
  return Number.isNaN(d.getTime()) ? null : d;
}

function normalizeCedulaToNumber(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).replace(/\D/g, "").trim();
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function clamp(n, min, max) {
  const x = Number(n);
  if (!Number.isFinite(x)) return min;
  return Math.min(Math.max(x, min), max);
}

// ======================================================
// Estilos (aprox al diseño de tu Excel)
// ======================================================
function applyBordersAll(ws) {
  ws.eachRow({ includeEmpty: true }, (row) => {
    row.eachCell({ includeEmpty: true }, (cell) => {
      cell.border = {
        top: { style: "thin" },
        bottom: { style: "thin" },
        left: { style: "thin" },
        right: { style: "thin" }
      };
    });
  });
}

function autoFitColumns(ws) {
  ws.columns.forEach((col) => {
    let max = 10;
    col.eachCell({ includeEmpty: true }, (cell) => {
      const v = cell.value == null ? "" : String(cell.value);
      max = Math.max(max, v.length + 2);
    });
    col.width = Math.min(Math.max(max, 10), 60);
  });
}

function styleSectionRow(ws) {
  const row = ws.getRow(2);
  row.height = 22;

  row.eachCell((cell) => {
    cell.font = { bold: true, size: 11, color: { argb: "FF000000" } };
    cell.alignment = { vertical: "middle", horizontal: "center", wrapText: true };
    cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFFFFFFF" } };
  });

  // Secciones (con ITEM=A)
  // K-M: presupuesto
  // O-Q: garantizados
  // R-X: ventas + cumplimiento
  const fillGray = { type: "pattern", pattern: "solid", fgColor: { argb: "FFF2F2F2" } };
  const fillRed = { type: "pattern", pattern: "solid", fgColor: { argb: "FFFFC7CE" } };

  // 1er parte presupuesto
  for (const c of ["K", "L", "M"]) ws.getCell(`${c}2`).fill = fillGray;

  // validación garantizado
  for (const c of ["O", "P", "Q"]) ws.getCell(`${c}2`).fill = fillGray;

  // resultado ventas
  for (const c of ["R", "S", "T", "U", "V", "W", "X"]) ws.getCell(`${c}2`).fill = fillRed;
}

function styleHeaderRow(ws) {
  const row = ws.getRow(3);
  row.height = 60;

  const baseHeaderFill = { type: "pattern", pattern: "solid", fgColor: { argb: "FF1F1F1F" } };
  const baseHeaderFont = { bold: true, size: 10, color: { argb: "FFFFFFFF" } };

  row.eachCell((cell) => {
    cell.fill = baseHeaderFill;
    cell.font = baseHeaderFont;
    cell.alignment = { vertical: "middle", horizontal: "center", wrapText: true };
  });

  // Colores por sección (aprox al excel que mostraste)
  // L y O: naranja (días)
  const orange = { type: "pattern", pattern: "solid", fgColor: { argb: "FFF4B183" } };
  // M y Q: verde suave (prorrateo / garantizado con novedades)
  const greenSoft = { type: "pattern", pattern: "solid", fgColor: { argb: "FFC6E0B4" } };
  // P: verde fuerte (garantizado para comisionar)
  const greenStrong = { type: "pattern", pattern: "solid", fgColor: { argb: "FF92D050" } };
  // R-T + W-X: rojo (ventas + cumple)
  const red = { type: "pattern", pattern: "solid", fgColor: { argb: "FFFF0000" } };
  // U-V: naranja/amarillo (diferencias)
  const diff = { type: "pattern", pattern: "solid", fgColor: { argb: "FFFFC000" } };

  ws.getCell("L3").fill = orange; // días (validar novedades)
  ws.getCell("M3").fill = greenSoft; // prorrateo
  ws.getCell("O3").fill = orange; // días (2da parte)
  ws.getCell("P3").fill = greenStrong; // garantizado comisionar
  ws.getCell("Q3").fill = greenSoft; // garantizado con novedades

  for (const c of ["R", "S", "T"]) ws.getCell(`${c}3`).fill = red;
  for (const c of ["U", "V"]) ws.getCell(`${c}3`).fill = diff;
  for (const c of ["W", "X"]) ws.getCell(`${c}3`).fill = red;
}

function setNumberFormats(ws) {
  // Con ITEM=A, CEDULA=B
  ws.getColumn("A").numFmt = "0";
  ws.getColumn("B").numFmt = "0";

  // Fechas
  ws.getColumn("G").numFmt = "yyyy-mm-dd";
  ws.getColumn("H").numFmt = "yyyy-mm-dd";

  // Presupuesto/Prorrateo/Garantizados
  ws.getColumn("K").numFmt = "0.00";
  ws.getColumn("L").numFmt = "0";
  ws.getColumn("M").numFmt = "0.00";
  ws.getColumn("O").numFmt = "0";
  ws.getColumn("P").numFmt = "0.00";
  ws.getColumn("Q").numFmt = "0.00";

  // Ventas + difs
  for (const c of ["R", "S", "T", "U", "V"]) ws.getColumn(c).numFmt = "0";
}
function addComplianceConditionalFormatting(ws) {
  // Datos empiezan en fila 4 (porque congelamos hasta fila 3)
  const startRow = 4;
  const endRow = 5000; // suficientemente alto para nómina

  // Columnas W y X
  for (const col of ["W", "X"]) {
    const ref = `${col}${startRow}:${col}${endRow}`;

    ws.addConditionalFormatting({
      ref,
      rules: [
        // CUMPLE = verde
        {
          type: "expression",
          formulae: [`${col}${startRow}="CUMPLE"`],
          style: {
            fill: { type: "pattern", pattern: "solid", fgColor: { argb: "FFC6EFCE" } }, // verde suave
            font: { color: { argb: "FF006100" }, bold: true }
          }
        },
        // NO CUMPLE = rojo
        {
          type: "expression",
          formulae: [`${col}${startRow}="NO CUMPLE"`],
          style: {
            fill: { type: "pattern", pattern: "solid", fgColor: { argb: "FFFFC7CE" } }, // rojo suave
            font: { color: { argb: "FF9C0006" }, bold: true }
          }
        },
        // NO APLICA = gris (opcional)
        {
          type: "expression",
          formulae: [`${col}${startRow}="NO APLICA"`],
          style: {
            fill: { type: "pattern", pattern: "solid", fgColor: { argb: "FFE7E6E6" } },
            font: { color: { argb: "FF404040" }, bold: true }
          }
        }
      ]
    });
  }
}


function addNominaSheet(wb, sheetName) {
  const ws = wb.addWorksheet(sheetName);

  // Fila 1 vacía (similar al archivo ejemplo)
  ws.addRow([]);

  // Fila 2: secciones (con merges)
  const row2 = ws.addRow(new Array(24).fill(""));

  // Columnas (24):
  // A ITEM
  // B CEDULA
  // C NOMBRE
  // D CONTRATADO
  // E DISTRITO
  // F DISTRITO CLARO
  // G FECHA INICIO
  // H FECHA FIN
  // I NOVEDADES
  // J ESTADO
  // K PRESUPUESTO
  // L DIAS (validar)
  // M PRORRATEO
  // N (separador)
  // O DIAS (validar)
  // P GARANTIZADO COMISIONAR
  // Q GARANTIZADO CON NOVEDADES
  // R Ventas distrito
  // S Ventas fuera
  // T Total ventas
  // U Dif distrito
  // V Dif totales
  // W Cumple zonificado
  // X Cumple global

  ws.getCell("K2").value = "1ER PARTE ENVIO PRESUPUESTO";
  ws.mergeCells("K2:M2");

  ws.getCell("O2").value = "1ER PARTE (VALIDACION DE GARANTIZADO PARA COMISIONAR) ojo formular";
  ws.mergeCells("O2:Q2");

  ws.getCell("R2").value = "3ER PARTE RESULTADO DE VENTAS (CRUCE REALIZA EL SISTEMA CON SIAPP)";
  ws.mergeCells("R2:X2");

  // Fila 3: headers exactos (con ITEM)
  ws.addRow([
    "ITEM",
    "CEDULA",
    "NOMBRE DE FUNCIONARIO",
    "CONTRATADO",
    "DISTRITO",
    "DISTRITO CLARO",
    "FECHA INICIO CONTRATO",
    "FECHA FIN CONTRATO",
    "NOVEDADES",
    "ESTADO",
    "PRESUPUESTO MES",
    "DIAS LABORADOS AL 31 MES ( VALIDAR NOVEDADES DEL MES)",
    "PRORRATEO SEGÚN NOVEDADES PARA CUMPLIMIENTO",
    "", // separador como tu ejemplo
    "DIAS LABORADOS AL 31 MES ( VALIDAR NOVEDADES DEL MES)",
    "GARANTIZADO PARA COMISIONAR",
    "GARANTIZADO AL 31 AGOSTO ( CON NOVEDADES)",
    "Ventas en el Distrito",
    "Ventas Fuera del Distrito",
    "TOTAL VENTAS",
    "DIFERENCIA VENTAS EN DISTRITO",
    "DIFERENCIA VENTAS TOTALES",
    "SI CUMPLE/ NO CUMPLE ( DISTRITO ZONIFICADO)",
    "SI CUMPLE/ NO CUMPLE ( NO DISCRIMINA DISTRITO)"
  ]);

  styleSectionRow(ws);
  styleHeaderRow(ws);
  setNumberFormats(ws);
  addComplianceConditionalFormatting(ws);


  // Congelar
  ws.views = [{ state: "frozen", xSplit: 0, ySplit: 3 }];

  return ws;
}

// ======================================================
// EXPORT NOMINA (2 HOJAS)
// Endpoint: /api/export/nomina?period=YYYY-MM
// ======================================================
export async function exportNominaController(req, res) {
  try {
    const { period } = req.query;

    if (!period || !/^\d{4}-\d{1,2}$/.test(String(period))) {
      return res.status(400).json({ ok: false, error: "Periodo inválido. Use YYYY-MM" });
    }

    const [yyRaw, mmRaw] = String(period).split("-").map(Number);
    const yy = yyRaw;
    const mm = mmRaw;

    if (!Number.isFinite(yy) || !Number.isFinite(mm) || mm < 1 || mm > 12) {
      return res.status(400).json({ ok: false, error: "Periodo inválido. Use YYYY-MM" });
    }

    const periodStr = `${yy}-${pad2(mm)}`;

    // month dim
    const { rows: dimRows } = await pool.query(
      `
      SELECT
        make_date($1::int,$2::int,1)::date AS month_start,
        (make_date($1::int,$2::int,1) + interval '1 month' - interval '1 day')::date AS month_end,
        EXTRACT(DAY FROM (make_date($1::int,$2::int,1) + interval '1 month' - interval '1 day'))::int AS days_in_month
      `,
      [yy, mm]
    );

    const monthStart = dimRows[0].month_start;
    const monthEnd = dimRows[0].month_end;
    const daysInMonth = Number(dimRows?.[0]?.days_in_month || 30);

    // ============================================================
    // HOJA 1: EN SISTEMA (con user) => fuente ventas = core.progress
    // Universo: usuarios que aparecen en progress del periodo
    // (Si quieres ampliar a user_monthly también, te dejo el WHERE listo)
    // ============================================================
    const { rows: nominaUsers } = await pool.query(
      `
      WITH nov AS (
        SELECT n.user_id,
          STRING_AGG(
            n.novelty_type || ' ' ||
            to_char(n.start_date,'DD/MM/YYYY') || ' AL ' ||
            to_char(n.end_date,'DD/MM/YYYY'),
            ' | '
          ) AS novedades
        FROM core.user_novelties n
        WHERE (n.start_date, n.end_date)
          OVERLAPS ($3::date, $4::date)
        GROUP BY n.user_id
      ),
      base AS (
        SELECT
          u.id AS user_id,
          u.document_id::text AS cedula,
          u.name AS funcionario,
          u.district AS distrito,
          u.district_claro AS distrito_claro,
          u.contract_start,
          u.contract_end,
          u.contract_status,
          u.active,

          um.presupuesto_mes,
          um.dias_laborados,
          um.prorrateo,

          p.real_in_count::int   AS ventas_distrito,
          p.real_out_count::int  AS ventas_fuera,
          p.real_total_count::int AS total_ventas,

          COALESCE(nov.novedades,'') AS novedades
        FROM core.progress p
        JOIN core.users u ON u.id = p.user_id
        LEFT JOIN core.user_monthly um
          ON um.user_id = u.id AND um.period_year = $1 AND um.period_month = $2
        LEFT JOIN nov ON nov.user_id = u.id
        WHERE p.period_year = $1 AND p.period_month = $2
        -- Si en algún mes quieres incluir también gente de user_monthly aunque no tenga ventas:
        -- OR um.user_id IS NOT NULL
      )
      SELECT *
      FROM base
      ORDER BY total_ventas DESC, funcionario ASC
      `,
      [yy, mm, monthStart, monthEnd]
    );

    // ============================================================
    // HOJA 2: FUERA SISTEMA (sin user) => ventas desde full_sales
    // ============================================================
    const { rows: fueraSistema } = await pool.query(
      `
      WITH unknown AS (
        SELECT
          regexp_replace(fs.idasesor::text, '\\D','', 'g') AS cedula_norm,
          MAX(fs.nombreasesor) AS nombre,
          COUNT(*)::int AS total_ventas
        FROM siapp.full_sales fs
        LEFT JOIN core.users u
          ON regexp_replace(u.document_id::text, '\\D','', 'g')
           = regexp_replace(fs.idasesor::text, '\\D','', 'g')
        WHERE fs.period_year = $1
          AND fs.period_month = $2
          AND fs.idasesor IS NOT NULL
          AND u.id IS NULL
        GROUP BY 1
      )
      SELECT *
      FROM unknown
      ORDER BY total_ventas DESC, nombre ASC NULLS LAST
      `,
      [yy, mm]
    );

    // ============================================================
    // Construcción Excel
    // ============================================================
    const wb = new ExcelJS.Workbook();

    // Hoja 1
    const wsNomina = addNominaSheet(wb, "Archivo Nomina");

    let item = 1;
    for (const r of nominaUsers) {
      // CONTRATADO: overlap contrato con mes (no depende de user_monthly)
      const contractStart = r.contract_start ? new Date(r.contract_start) : null;
      const contractEnd = r.contract_end ? new Date(r.contract_end) : null;
      const overlaps =
        contractStart &&
        contractStart <= new Date(monthEnd) &&
        (!contractEnd || contractEnd >= new Date(monthStart));

      const contratado = "SI";


      // ESTADO: preferir contract_status si viene, si no derivar
      const estado = r.contract_status ?? (overlaps ? "ACTIVO" : "RETIRADO");

      const presupuestoMes = r.presupuesto_mes != null ? Number(r.presupuesto_mes) : 0;

      const diasLaborados = r.dias_laborados != null && Number.isFinite(Number(r.dias_laborados))
        ? Number(r.dias_laborados)
        : daysInMonth;

      // PRORRATEO: usar um.prorrateo o fallback (presupuesto * dias/diasMes)
      let prorrateo = 0;
      if (r.prorrateo != null && Number.isFinite(Number(r.prorrateo))) {
        prorrateo = Number(r.prorrateo);
      } else if (presupuestoMes > 0) {
        prorrateo = (presupuestoMes * diasLaborados) / daysInMonth;
      } else {
        prorrateo = 0;
      }

      // Garantizados
      const garantizadoParaComisionar = presupuestoMes;
      const garantizadoConNovedades = prorrateo;

      const ventasDistrito = Number(r.ventas_distrito || 0);
      const ventasFuera = Number(r.ventas_fuera || 0);
      const totalVentas = Number(r.total_ventas || 0);

      // Diferencias (como tu ejemplo)
      const difDistrito = Math.trunc(ventasDistrito - garantizadoConNovedades);
      const difTotal = Math.trunc(totalVentas - garantizadoConNovedades);

      // Cumple
      const cumpleZonificado =
        garantizadoConNovedades > 0
          ? (ventasDistrito >= garantizadoConNovedades ? "CUMPLE" : "NO CUMPLE")
          : "NO APLICA";

      const cumpleGlobal =
        garantizadoConNovedades > 0
          ? (totalVentas >= garantizadoConNovedades ? "CUMPLE" : "NO CUMPLE")
          : "NO APLICA";

      wsNomina.addRow([
        item++,
        normalizeCedulaToNumber(r.cedula),
        r.funcionario ?? "",
        contratado,
        r.distrito ?? "",
        r.distrito_claro ?? "",
        toCODate(r.contract_start),
        toCODate(r.contract_end),
        r.novedades ?? "",
        estado,
        presupuestoMes,
        diasLaborados,
        prorrateo,
        "", // separador
        diasLaborados,
        garantizadoParaComisionar,
        garantizadoConNovedades,
        ventasDistrito,
        ventasFuera,
        totalVentas,
        difDistrito,
        difTotal,
        cumpleZonificado,
        cumpleGlobal
      ]);
    }

    // Hoja 2 (mismo formato)
    const wsFuera = addNominaSheet(wb, "Fuera Sistema");

    let item2 = 1;
    for (const r of fueraSistema) {
      const totalVentas = Number(r.total_ventas || 0);

      // En fuera sistema:
      // - No hay distrito usuario => no se puede zonificar => todo lo tratamos como "fuera distrito"
      const ventasDistrito = 0;
      const ventasFuera = totalVentas;

      const garantizadoParaComisionar = 0;
      const garantizadoConNovedades = 0;

      wsFuera.addRow([
        item2++,
        normalizeCedulaToNumber(r.cedula_norm),
        r.nombre ?? "",
        "NO",
        "",
        "",
        null,
        null,
        "",
        "FUERA SISTEMA",
        0,
        daysInMonth,
        0,
        "",
        daysInMonth,
        garantizadoParaComisionar,
        garantizadoConNovedades,
        ventasDistrito,
        ventasFuera,
        totalVentas,
        0,
        0,
        "NO APLICA",
        "NO APLICA"
      ]);
    }

    // Formatos finales
for (const ws of [wsNomina, wsFuera]) {
  applyBordersAll(ws);
  autoFitColumns(ws);

  // ---------- Forzar anchos (RH-friendly) ----------
  // Fechas (G,H) angostas
  ws.getColumn("G").width = 12;
  ws.getColumn("H").width = 12;
  ws.getColumn("J").width = 15; // Estado

  // Días / prorrateo / garantizados (K-Q) más angostos
  ws.getColumn("K").width = 12; // presupuesto
  ws.getColumn("L").width = 18;// días
  ws.getColumn("M").width = 18; // prorrateo
  ws.getColumn("O").width = 18; // días (2)
  ws.getColumn("P").width = 18; // garantizado
  ws.getColumn("Q").width = 18; // garantizado con novedades

  // Ventas / diferencias (R-V) angostas
  ws.getColumn("R").width = 10;
  ws.getColumn("S").width = 10;
  ws.getColumn("T").width = 10;
  ws.getColumn("U").width = 12;
  ws.getColumn("V").width = 12;

  // Cumple (W,X) angostas
  ws.getColumn("W").width = 18;
  ws.getColumn("X").width = 18;

  // Campos largos controlados
  ws.getColumn("C").width = 28; // nombre
  ws.getColumn("I").width = 40; // novedades

}


    // Hoja resumen (opcional pero útil para validar)
    const wsResumen = wb.addWorksheet("RESUMEN");
    wsResumen.addRow(["METRICA", "VALOR"]);
    wsResumen.getRow(1).font = { bold: true };

    const totalFull = await pool.query(
      `SELECT COUNT(*)::int AS filas FROM siapp.full_sales WHERE period_year=$1 AND period_month=$2`,
      [yy, mm]
    );

    const withUser = await pool.query(
      `
      SELECT COUNT(*)::int AS filas
      FROM siapp.full_sales fs
      JOIN core.users u ON u.document_id::text = fs.idasesor::text
      WHERE fs.period_year=$1 AND fs.period_month=$2
      `,
      [yy, mm]
    );

    wsResumen.addRow(["PERIODO", periodStr]);
    wsResumen.addRow(["TOTAL DE CONEXIONES", Number(totalFull.rows[0]?.filas || 0)]);
    wsResumen.addRow(["CONEXIONES POR USUARIOS EN NOMINA", Number(withUser.rows[0]?.filas || 0)]);
    wsResumen.addRow(["CONEXIONES POR USUARIOS FUERA DE NOMINA", fueraSistema.reduce((a, x) => a + Number(x.total_ventas || 0), 0)]);

    autoFitColumns(wsResumen);
    applyBordersAll(wsResumen);

    // Export
    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", `attachment; filename="Nomina-${periodStr}.xlsx"`);

    await wb.xlsx.write(res);
    res.end();
  } catch (e) {
    console.error("[EXPORT NOMINA] ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
}

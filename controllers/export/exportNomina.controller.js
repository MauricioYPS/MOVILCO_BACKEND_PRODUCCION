// controllers/export/nomina/exportNomina.controller.js
import ExcelJS from "exceljs";
import pool from "../../config/database.js";
import { promoteSiappFromFullSales } from "../../services/promote.siapp.service.js";

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

  const fillGray = { type: "pattern", pattern: "solid", fgColor: { argb: "FFF2F2F2" } };
  const fillRed = { type: "pattern", pattern: "solid", fgColor: { argb: "FFFFC7CE" } };

  for (const c of ["K", "L", "M"]) ws.getCell(`${c}2`).fill = fillGray;
  for (const c of ["O", "P", "Q"]) ws.getCell(`${c}2`).fill = fillGray;
  for (const c of ["R", "S", "T", "U", "V", "W", "X", "Y"]) ws.getCell(`${c}2`).fill = fillRed;
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

  const orange = { type: "pattern", pattern: "solid", fgColor: { argb: "FFF4B183" } };
  const greenSoft = { type: "pattern", pattern: "solid", fgColor: { argb: "FFC6E0B4" } };
  const greenStrong = { type: "pattern", pattern: "solid", fgColor: { argb: "FF92D050" } };
  const red = { type: "pattern", pattern: "solid", fgColor: { argb: "FFFF0000" } };
  const diff = { type: "pattern", pattern: "solid", fgColor: { argb: "FFFFC000" } };

  ws.getCell("L3").fill = orange;
  ws.getCell("M3").fill = greenSoft;
  ws.getCell("O3").fill = orange;
  ws.getCell("P3").fill = greenStrong;
  ws.getCell("Q3").fill = greenSoft;

  for (const c of ["R", "S", "T"]) ws.getCell(`${c}3`).fill = red;
  for (const c of ["U", "V"]) ws.getCell(`${c}3`).fill = diff;
  for (const c of ["X", "Y"]) ws.getCell(`${c}3`).fill = red;
}

function setNumberFormats(ws) {
  ws.getColumn("A").numFmt = "0";
  ws.getColumn("B").numFmt = "0";

  ws.getColumn("G").numFmt = "yyyy-mm-dd";
  ws.getColumn("H").numFmt = "yyyy-mm-dd";

  ws.getColumn("K").numFmt = "0";
  ws.getColumn("L").numFmt = "0";
  ws.getColumn("M").numFmt = "0.00";

  ws.getColumn("O").numFmt = "0";
  ws.getColumn("P").numFmt = "0";
  ws.getColumn("Q").numFmt = "0.00";

  for (const c of ["R", "S", "T", "U", "V", "W"]) ws.getColumn(c).numFmt = "0";
}

function addComplianceConditionalFormatting(ws) {
  const startRow = 4;
  const endRow = 5000;

  for (const col of ["X", "Y"]) {
    const ref = `${col}${startRow}:${col}${endRow}`;

    ws.addConditionalFormatting({
      ref,
      rules: [
        {
          type: "expression",
          formulae: [`${col}${startRow}="CUMPLE"`],
          style: {
            fill: { type: "pattern", pattern: "solid", fgColor: { argb: "FFC6EFCE" } },
            font: { color: { argb: "FF006100" }, bold: true }
          }
        },
        {
          type: "expression",
          formulae: [`${col}${startRow}="NO CUMPLE"`],
          style: {
            fill: { type: "pattern", pattern: "solid", fgColor: { argb: "FFFFC7CE" } },
            font: { color: { argb: "FF9C0006" }, bold: true }
          }
        },
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

  ws.addRow([]);
  ws.addRow(new Array(25).fill(""));

  ws.getCell("K2").value = "1ER PARTE ENVIO PRESUPUESTO";
  ws.mergeCells("K2:M2");

  ws.getCell("O2").value = "1ER PARTE (VALIDACION DE GARANTIZADO PARA COMISIONAR) ojo formular";
  ws.mergeCells("O2:Q2");

  ws.getCell("R2").value = "3ER PARTE RESULTADO DE VENTAS (CRUCE REALIZA EL SISTEMA CON SIAPP)";
  ws.mergeCells("R2:Y2");

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
    "",
    "DIAS LABORADOS AL 31 MES ( VALIDAR NOVEDADES DEL MES)",
    "GARANTIZADO PARA COMISIONAR",
    "GARANTIZADO AL 31 AGOSTO ( CON NOVEDADES)",
    "Ventas en el Distrito",
    "Ventas Fuera del Distrito",
    "Ventas No Zonificadas",
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

  ws.views = [{ state: "frozen", xSplit: 0, ySplit: 3 }];

  return ws;
}

// ======================================================
// (B) AUTOMÁTICO: BACKFILL/REFRESH USER_MONTHLY + RECALC PROGRESS
//   - Respeta kpi.dias_laborados_manual (override)
// ======================================================
async function ensureMonthlyAndProgress({ yy, mm, periodStr }) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const backfill = await client.query(
      `
      WITH bounds AS (
        SELECT
          make_date($1::int,$2::int,1)::date AS month_start,
          (make_date($1::int,$2::int,1) + interval '1 month' - interval '1 day')::date AS month_end,
          EXTRACT(DAY FROM (make_date($1::int,$2::int,1) + interval '1 month' - interval '1 day'))::int AS days_in_month
      ),
      users_sel AS (
        SELECT u.id AS user_id
        FROM core.users u
        WHERE u.role IN ('ASESORIA')
          AND u.active = true
      ),
      budgets AS (
        SELECT b.user_id, COALESCE(b.budget_amount,0)::numeric AS presupuesto_mes
        FROM core.budgets b
        WHERE b.period = $3
      ),
      nov_days AS (
        SELECT
          n.user_id,
          COUNT(DISTINCT gs::date)::int AS non_worked_days
        FROM core.user_novelties n
        CROSS JOIN bounds b
        CROSS JOIN LATERAL generate_series(
          GREATEST(n.start_date::date, b.month_start),
          LEAST(n.end_date::date, b.month_end),
          interval '1 day'
        ) gs
        WHERE n.start_date::date <= b.month_end
          AND n.end_date::date >= b.month_start
        GROUP BY n.user_id
      ),
      manual_days AS (
        SELECT md.user_id, md.dias::int AS dias_manual
        FROM kpi.dias_laborados_manual md
        WHERE md.period_year = $1
          AND md.period_month = $2
      ),
      calc AS (
        SELECT
          us.user_id,
          COALESCE(bu.presupuesto_mes, 0)::numeric AS presupuesto_mes,
          (SELECT days_in_month FROM bounds)::int AS days_in_month,
          COALESCE(nd.non_worked_days, 0)::int AS non_worked_days,
          -- auto
          GREATEST((SELECT days_in_month FROM bounds)::int - COALESCE(nd.non_worked_days,0)::int, 0)::int AS dias_auto,
          -- manual override si existe
          md.dias_manual
        FROM users_sel us
        LEFT JOIN budgets bu ON bu.user_id = us.user_id
        LEFT JOIN nov_days nd ON nd.user_id = us.user_id
        LEFT JOIN manual_days md ON md.user_id = us.user_id
      ),
      final_calc AS (
        SELECT
          c.user_id,
          c.presupuesto_mes,
          c.days_in_month,
          COALESCE(c.dias_manual, c.dias_auto)::int AS dias_laborados_final
        FROM calc c
      )
      INSERT INTO core.user_monthly (
        user_id, period_year, period_month,
        presupuesto_mes, dias_laborados, prorrateo,
        updated_at
      )
      SELECT
        fc.user_id,
        $1::int AS period_year,
        $2::int AS period_month,
        fc.presupuesto_mes,
        fc.dias_laborados_final,
        CASE
          WHEN fc.presupuesto_mes > 0 AND fc.days_in_month > 0
          THEN ROUND((fc.presupuesto_mes * fc.dias_laborados_final::numeric) / fc.days_in_month::numeric, 4)
          ELSE 0
        END AS prorrateo,
        now()
      FROM final_calc fc
      ON CONFLICT (user_id, period_year, period_month)
      DO UPDATE SET
        presupuesto_mes = EXCLUDED.presupuesto_mes,
        dias_laborados  = EXCLUDED.dias_laborados,
        prorrateo       = EXCLUDED.prorrateo,
        updated_at      = now()
      `,
      [yy, mm, periodStr]
    );

    await client.query("COMMIT");

    const promote = await promoteSiappFromFullSales({ period_year: yy, period_month: mm });

    return {
      monthly_backfill: {
        ok: true,
        upsert_attempted: Number(backfill?.rowCount || 0)
      },
      progress_recalc: promote
    };
  } catch (e) {
    try {
      await client.query("ROLLBACK");
    } catch (_) {}
    throw e;
  } finally {
    client.release();
  }
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

    // ============================================================
    // (B) AUTOMÁTICO: antes de exportar, asegurar monthly y progress
    //  - ahora respeta manual_days
    // ============================================================
    const auto = await ensureMonthlyAndProgress({ yy, mm, periodStr });

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
    const daysInMonthFallback = Number(dimRows?.[0]?.days_in_month || 30);

    // ============================================================
    // HOJA 1: EN SISTEMA (TODOS LOS USERS)
    //  - Se apoya en core.user_monthly (ya calculado y con manual)
    // ============================================================
    const { rows: nominaUsers } = await pool.query(
      `
      WITH bounds AS (
        SELECT
          make_date($1::int,$2::int,1)::date AS month_start,
          (make_date($1::int,$2::int,1) + interval '1 month' - interval '1 day')::date AS month_end,
          EXTRACT(DAY FROM (make_date($1::int,$2::int,1) + interval '1 month' - interval '1 day'))::int AS days_in_month
      ),
      users_sel AS (
        SELECT
          u.id,
          u.document_id::text AS cedula,
          u.name,
          u.district,
          u.district_claro,
          u.contract_start,
          u.contract_end,
          u.contract_status,
          u.active
        FROM core.users u
        WHERE u.role IN ('ASESORIA')
          AND u.active = true
      ),
      nov_days AS (
        SELECT
          n.user_id,
          COUNT(DISTINCT gs::date)::int AS non_worked_days,
          STRING_AGG(
            n.novelty_type || ' ' ||
            to_char(n.start_date,'DD/MM/YYYY') || ' AL ' ||
            to_char(n.end_date,'DD/MM/YYYY'),
            ' | '
            ORDER BY n.start_date ASC
          ) AS novedades
        FROM core.user_novelties n
        CROSS JOIN bounds b
        CROSS JOIN LATERAL generate_series(
          GREATEST(n.start_date::date, b.month_start),
          LEAST(n.end_date::date, b.month_end),
          interval '1 day'
        ) gs
        WHERE n.start_date::date <= b.month_end
          AND n.end_date::date >= b.month_start
        GROUP BY n.user_id
      )
      SELECT
        u.id AS user_id,
        u.cedula,
        u.name AS funcionario,
        u.district AS distrito,
        u.district_claro AS distrito_claro,
        u.contract_start,
        u.contract_end,
        u.contract_status,
        u.active,

        COALESCE(um.presupuesto_mes, bu.budget_amount, 0)::numeric AS presupuesto_mes,
        COALESCE(um.dias_laborados, 0)::int AS dias_laborados,
        COALESCE(um.prorrateo, 0)::numeric AS prorrateo,

        COALESCE(nd.non_worked_days, 0)::int AS non_worked_days,
        COALESCE(nd.novedades,'') AS novedades,

        COALESCE(p.real_in_count, 0)::int        AS ventas_distrito,
        COALESCE(p.real_out_count, 0)::int       AS ventas_fuera,
        COALESCE(p.real_unzoned_count, 0)::int   AS ventas_no_zonificadas,
        COALESCE(p.real_total_count, 0)::int     AS total_ventas,

        (SELECT days_in_month FROM bounds) AS days_in_month
      FROM users_sel u
      LEFT JOIN core.user_monthly um
        ON um.user_id = u.id
       AND um.period_year = $1
       AND um.period_month = $2
      LEFT JOIN core.budgets bu
        ON bu.user_id = u.id
       AND bu.period = $3
      LEFT JOIN nov_days nd
        ON nd.user_id = u.id
      LEFT JOIN core.progress p
        ON p.user_id = u.id
       AND p.period_year = $1
       AND p.period_month = $2
      ORDER BY total_ventas DESC, funcionario ASC
      `,
      [yy, mm, periodStr]
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
      const overlaps =
        r.contract_start &&
        new Date(r.contract_start) <= new Date(monthEnd) &&
        (!r.contract_end || new Date(r.contract_end) >= new Date(monthStart));

      const contratado = "SI";
      const estado = r.contract_status ?? (overlaps ? "ACTIVO" : "RETIRADO");

      const daysInMonth = Number(r.days_in_month || daysInMonthFallback || 30);

      // IMPORTANTÍSIMO: ahora vienen de core.user_monthly (ya respeta manual)
      const diasLaborados = Number(r.dias_laborados ?? 0);
      const presupuestoMes = r.presupuesto_mes != null ? Number(r.presupuesto_mes) : 0;
      const prorrateo = r.prorrateo != null ? Math.round(Number(r.prorrateo)) : 0; // redondeo

      const garantizadoParaComisionar = presupuestoMes;
      const garantizadoConNovedades = prorrateo;

      const ventasDistrito = Number(r.ventas_distrito || 0);
      const ventasFuera = Number(r.ventas_fuera || 0);
      const ventasNoZon = Number(r.ventas_no_zonificadas || 0);
      const totalVentas = Number(r.total_ventas || 0);

      const difDistrito = Math.trunc(ventasDistrito - garantizadoConNovedades);
      const difTotal = Math.trunc(totalVentas - garantizadoConNovedades);

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
        "",
        diasLaborados,
        garantizadoParaComisionar,
        garantizadoConNovedades,
        ventasDistrito,
        ventasFuera,
        ventasNoZon,
        totalVentas,
        difDistrito,
        difTotal,
        cumpleZonificado,
        cumpleGlobal
      ]);
    }

    // Hoja 2
    const wsFuera = addNominaSheet(wb, "Fuera Sistema");

    let item2 = 1;
    for (const r of fueraSistema) {
      const totalVentas = Number(r.total_ventas || 0);

      const ventasDistrito = 0;
      const ventasFuera = totalVentas;
      const ventasNoZon = 0;

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
        daysInMonthFallback,
        0,
        "",
        daysInMonthFallback,
        garantizadoParaComisionar,
        garantizadoConNovedades,
        ventasDistrito,
        ventasFuera,
        ventasNoZon,
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

      ws.getColumn("G").width = 12;
      ws.getColumn("H").width = 12;
      ws.getColumn("J").width = 15;

      ws.getColumn("K").width = 12;
      ws.getColumn("L").width = 18;
      ws.getColumn("M").width = 18;
      ws.getColumn("O").width = 18;
      ws.getColumn("P").width = 18;
      ws.getColumn("Q").width = 18;

      ws.getColumn("R").width = 10;
      ws.getColumn("S").width = 10;
      ws.getColumn("T").width = 14;
      ws.getColumn("U").width = 10;
      ws.getColumn("V").width = 12;
      ws.getColumn("W").width = 12;

      ws.getColumn("X").width = 18;
      ws.getColumn("Y").width = 18;

      ws.getColumn("C").width = 28;
      ws.getColumn("I").width = 40;
    }

    // Hoja resumen
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

    const totalNoZon = await pool.query(
      `
      SELECT COALESCE(SUM(p.real_unzoned_count),0)::int AS total_no_zonificadas
      FROM core.progress p
      JOIN core.users u ON u.id = p.user_id
      WHERE p.period_year=$1 AND p.period_month=$2
        AND u.role='ASESORIA' AND u.active=true
      `,
      [yy, mm]
    );

    wsResumen.addRow(["PERIODO", periodStr]);
    wsResumen.addRow(["TOTAL DE CONEXIONES ", Number(totalFull.rows[0]?.filas || 0)]);
    wsResumen.addRow(["CONEXIONES POR USUARIOS EN NOMINA", Number(withUser.rows[0]?.filas || 0)]);
    wsResumen.addRow([
      "CONEXIONES POR USUARIOS FUERA DE NOMINA",
      fueraSistema.reduce((a, x) => a + Number(x.total_ventas || 0), 0)
    ]);
    wsResumen.addRow(["TOTAL VENTAS NO ZONIFICADAS", Number(totalNoZon.rows[0]?.total_no_zonificadas || 0)]);

    // wsResumen.addRow([
    //   "AUTO: MONTHLY BACKFILL (UPSERT ATTEMPTED)",
    //   Number(auto?.monthly_backfill?.upsert_attempted || 0)
    // ]);
    // wsResumen.addRow(["AUTO: PROGRESS RECALC (UPSERTED)", Number(auto?.progress_recalc?.upserted || 0)]);
    // wsResumen.addRow(["AUTO: PROGRESS MATCHED USERS", Number(auto?.progress_recalc?.matched_users || 0)]);
    // wsResumen.addRow(["AUTO: THRESHOLD %", Number(auto?.progress_recalc?.threshold_percent ?? 100)]);

    autoFitColumns(wsResumen);
    applyBordersAll(wsResumen);

    // Export
    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.setHeader("Content-Disposition", `attachment; filename="Nomina-${periodStr}.xlsx"`);

    await wb.xlsx.write(res);
    res.end();
  } catch (e) {
    console.error("[EXPORT NOMINA] ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
}

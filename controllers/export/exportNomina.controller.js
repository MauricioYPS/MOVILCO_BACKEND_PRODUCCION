import ExcelJS from "exceljs";
import pool from "../../config/database.js";

// ==============================================
// Helpers de estilo
// ==============================================

function styleHeader(ws) {
  const header = ws.getRow(1);
  header.height = 25;

  header.eachCell((cell) => {
    cell.fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "FFF2CC" }
    };
    cell.font = { bold: true, size: 12 };
    cell.alignment = { vertical: "middle", horizontal: "center", wrapText: true };
    cell.border = {
      top: { style: "thin" },
      bottom: { style: "thin" },
      left: { style: "thin" },
      right: { style: "thin" }
    };
  });
}

function applyFullBorders(ws) {
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
    // Respeta anchos definidos manualmente
    if (col.width) return;
    let max = 12;
    col.eachCell({ includeEmpty: true }, (cell) => {
      const v = cell.value ? String(cell.value) : "";
      max = Math.max(max, v.length + 2);
    });
    col.width = max;
  });
}

// ==============================================
// CONTROLADOR FINAL EXPORTAR NÓMINA
// ==============================================

export async function exportNominaController(req, res) {
  try {
    const { period } = req.query;

    if (!period || !/^\d{4}-\d{2}$/.test(period))
      return res.status(400).json({ ok: false, error: "Periodo inválido" });

    const [yy, mm] = period.split("-").map(Number);

    // =============================
    // Consultar resultados KPI
    // =============================
    const { rows } = await pool.query(
      `
      SELECT 
        kr.*, 
        u.document_id AS cedula,
        u.name AS funcionario,
        u.contract_start,
        u.contract_end,
        u.contract_status,
        u.district AS distrito,
        u.district_claro,
        u.presupuesto
      FROM kpi.kpi_resultados kr
      JOIN core.users u ON u.id = kr.asesor_id
      WHERE kr.period_year = $1 AND kr.period_month = $2
      ORDER BY u.document_id
      `,
      [yy, mm]
    );

    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet("Nomina");

    // ==========================================
    // Encabezados oficiales (CON NOVEDADES)
    // ==========================================
    const headers = [
      "CEDULA", "NOMBRE DE FUNCIONARIO", "CONTRATADO", "DISTRITO", "DISTRITO CLARO",
      "FECHA INICIO CONTRATO", "FECHA FIN CONTRATO", "NOVEDADES", "ESTADO",
      "PRESUPUESTO MES", "DIAS LABORADOS", "PRORRATEO", "GARANTIZADO",
      "VENTAS DISTRITO", "VENTAS FUERA", "TOTAL VENTAS", "DIFERENCIA DISTRITO",
      "DIFERENCIA TOTAL", "CUMPLE DISTRITO", "CUMPLE GLOBAL"
    ];

    ws.addRow(headers);

    // ==========================================
    // Agregar datos fila por fila
    // ==========================================
    for (const r of rows) {

      // ==========================================
      // CONSULTAR NOVEDADES DEL USUARIO
      // ==========================================
      const novQuery = `
        SELECT tipo, fecha_inicio, fecha_fin
        FROM kpi.novedades
        WHERE user_id = $1
        AND (
            (EXTRACT(YEAR FROM fecha_inicio) = $2 AND EXTRACT(MONTH FROM fecha_inicio) = $3)
         OR (EXTRACT(YEAR FROM fecha_fin) = $2 AND EXTRACT(MONTH FROM fecha_fin) = $3)
        )
        ORDER BY fecha_inicio
      `;

      const { rows: novedadesRows } = await pool.query(novQuery, [
        r.asesor_id,
        yy,
        mm
      ]);

      // Formato requerido: DESCRIPCIÓN + fechas
      const novedadesFormateadas = novedadesRows.map(n => {
        const fi = n.fecha_inicio ? new Date(n.fecha_inicio).toLocaleDateString("es-CO") : "";
        const ff = n.fecha_fin ? new Date(n.fecha_fin).toLocaleDateString("es-CO") : "";

        if (n.fecha_inicio && n.fecha_fin) {
          return `${n.tipo} DEL ${fi} AL ${ff}`;
        }
        return `${n.tipo} ${fi}`;
      });

      const novedadesTexto = novedadesFormateadas.join(", ");

      // ==========================================
      // Añadir fila completa
      // ==========================================
      ws.addRow([
        Number(r.cedula),
        r.funcionario,
        r.contract_status ?? "N/A",
        r.distrito,
        r.district_claro,
        r.contract_start,
        r.contract_end,
        novedadesTexto,      //  <<<<< NUEVA COLUMNA EXACTA
        r.estado,
        Number(r.presupuesto_mes ?? r.presupuesto ?? 13),
        Number(r.dias_laborados),
        Number(r.presupuesto_prorrateado),
        Number(r.presupuesto_prorrateado),
        Number(r.ventas_distrito),
        Number(r.ventas_fuera),
        Number(r.ventas_totales),
        Number(r.ventas_distrito - r.presupuesto_prorrateado),
        Number(r.ventas_totales - r.presupuesto_prorrateado),
        r.cumple_distrito,
        r.cumple_global
      ]);
    }

    // ==========================================
    // Aplicar estilos
    // ==========================================

    styleHeader(ws);

    //  GARANTIZADO -> AHORA ES COLUMNA 13
    ws.getColumn(13).eachCell((cell, row) => {
      if (row === 1) return;
      cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "E2F4DA" } };
    });

    // TOTAL VENTAS -> AHORA ES COLUMNA 16
    ws.getColumn(16).eachCell((cell, row) => {
      if (row === 1) return;
      cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FDE2D9" } };
    });

    const green = "C6EAC2";
    const red = "F4C7C3";

    // CUMPLE DISTRITO -> COLUMNA 19
    ws.getColumn(19).eachCell((cell, row) => {
      if (row === 1) return;
      if (String(cell.value).toUpperCase() === "TRUE") {
        cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: green } };
      } else {
        cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: red } };
      }
    });

    // CUMPLE GLOBAL -> COLUMNA 20
    ws.getColumn(20).eachCell((cell, row) => {
      if (row === 1) return;
      if (String(cell.value).toUpperCase() === "TRUE") {
        cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: green } };
      } else {
        cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: red } };
      }
    });
    // Ajustar manualmente columnas de fechas
    ws.getColumn(6).width = 14; // FECHA INICIO CONTRATO
    ws.getColumn(7).width = 14; // FECHA FIN CONTRATO


    applyFullBorders(ws);
    autoFitColumns(ws);

    // ==========================================
    // Exportar archivo
    // ==========================================
    res.setHeader("Content-Type", "application/vnd.openxmlformats");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="Nomina-${period}.xlsx"`
    );

    await wb.xlsx.write(res);
    res.end();

  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: e.message });
  }
}

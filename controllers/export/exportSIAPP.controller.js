import ExcelJS from "exceljs";
import pool from "../../config/database.js";

// ======================================================
// Helpers
// ======================================================

function styleHeader(ws) {
  const header = ws.getRow(1);
  header.height = 22;

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
      left: { style: "thin" },
      bottom: { style: "thin" },
      right: { style: "thin" }
    };
  });
}

function applyFullBorders(ws, rowCount, colCount) {
  for (let r = 1; r <= rowCount; r++) {
    ws.getRow(r).eachCell({ includeEmpty: true }, (cell) => {
      cell.border = {
        top: { style: "thin" },
        left: { style: "thin" },
        bottom: { style: "thin" },
        right: { style: "thin" }
      };
    });
  }
}

function autoFitColumns(ws) {
  ws.columns.forEach((col) => {
    let max = 10;
    col.eachCell({ includeEmpty: true }, (cell) => {
      const text = cell.value ? String(cell.value) : "";
      max = Math.max(max, text.length + 2);
    });
    col.width = max;
  });
}

// ======================================================
// CONTROLADOR EXPORTAR SIAPP
// ======================================================

export async function exportSIAPPController(req, res) {
  try {
    const { period } = req.query;

    if (!period || !/^\d{4}-\d{2}$/.test(period))
      return res.status(400).json({ ok: false, error: "Periodo inválido" });

    const [yy, mm] = period.split("-").map(Number);

    const { rows } = await pool.query(
      `
      SELECT *
      FROM siapp.all_sales_view
      WHERE (period_year = $1 AND period_month = $2)
         OR (EXTRACT(YEAR FROM fecha) = $1 AND EXTRACT(MONTH FROM fecha) = $2)
      `,
      [yy, mm]
    );

    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet("SIAPP");

    const headers = [
      "Estado_Liquidacion","Linea_Negocio","Cuenta","Ot","IdAsesor",
      "NombreAsesor","CantServ","TipoRed","Division","Area","Zona",
      "Poblacion","D_Distrito","Renta","Fecha","Venta","Tipo_Registro",
      "Estrato","Paquete_PVD","MINTIC","Tipo_Prodcuto","VentaConvergente",
      "Venta_Instale_DTH","SAC_FINAL","Cedula_Vendedor","Nombre_Vendedor",
      "Modalidad_Venta","Tipo_Vendedor","Tipo_Red_Comercial",
      "Nombre_Regional","Nombre_Comercial","Nombre_Lider",
      "Retencion_Control","Observ_Retencion","Tipo_Contrato",
      "Tarifa_Venta","Comision_Neta","Punto_Equilibrio"
    ];

    ws.addRow(headers);

    for (const r of rows) {
      ws.addRow([
        r.estado_liquidacion,
        r.linea_negocio,
        r.cuenta,
        r.ot,
        r.idasesor,
        r.nombreasesor,
        Number(r.cantserv),
        r.tipored,
        r.division,
        r.area,
        r.zona,
        r.poblacion,
        r.d_distrito,
        Number(r.renta),
        r.fecha,
        r.venta,
        r.tipo_registro,
        r.estrato,
        r.paquete_pvd,
        r.mintic,
        r.tipo_prodcuto,
        r.ventaconvergente,
        r.venta_instale_dth,
        r.sac_final,
        r.cedula_vendedor,
        r.nombre_vendedor,
        r.modalidad_venta,
        r.tipo_vendedor,
        r.tipo_red_comercial,
        r.nombre_regional,
        r.nombre_comercial,
        r.nombre_lider,
        r.retencion_control,
        r.observ_retencion,
        r.tipo_contrato,
        Number(r.tarifa_venta),
        Number(r.comision_neta),
        Number(r.punto_equilibrio)
      ]);
    }

    // Estilos
    styleHeader(ws);
    autoFitColumns(ws);

    // Convertir a tabla
    ws.addTable({
      name: "TablaSiapp",
      ref: "A1",
      headerRow: true,
      style: {
        theme: "TableStyleMedium2",
        showRowStripes: true,
      },
      columns: headers.map(h => ({ name: h })),
      rows: rows.map(r => [
        r.estado_liquidacion,
        r.linea_negocio,
        r.cuenta,
        r.ot,
        r.idasesor,
        r.nombreasesor,
        Number(r.cantserv),
        r.tipored,
        r.division,
        r.area,
        r.zona,
        r.poblacion,
        r.d_distrito,
        Number(r.renta),
        r.fecha,
        r.venta,
        r.tipo_registro,
        r.estrato,
        r.paquete_pvd,
        r.mintic,
        r.tipo_prodcuto,
        r.ventaconvergente,
        r.venta_instale_dth,
        r.sac_final,
        r.cedula_vendedor,
        r.nombre_vendedor,
        r.modalidad_venta,
        r.tipo_vendedor,
        r.tipo_red_comercial,
        r.nombre_regional,
        r.nombre_comercial,
        r.nombre_lider,
        r.retencion_control,
        r.observ_retencion,
        r.tipo_contrato,
        Number(r.tarifa_venta),
        Number(r.comision_neta),
        Number(r.punto_equilibrio)
      ])
    });

    // Bordes de toda la tabla
    applyFullBorders(ws, ws.rowCount, headers.length);

    // Exportar
    res.setHeader("Content-Type", "application/vnd.openxmlformats");
    res.setHeader("Content-Disposition",
      `attachment; filename="SIAPP-${period}.xlsx"`
    );

    await wb.xlsx.write(res);
    res.end();

  } catch (e) {
    console.error("EXPORT SIAPP ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
}

// controllers/export/exportSIAPP.controller.js
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

function applyFullBorders(ws, rowCount) {
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
    col.width = Math.min(Math.max(max, 10), 60);
  });
}

// ======================================================
// CONTROLADOR EXPORTAR SIAPP
// Endpoint: /api/export/siapp?period=YYYY-MM
// ======================================================
export async function exportSIAPPController(req, res) {
  try {
    const { period } = req.query;

    if (!period || !/^\d{4}-\d{2}$/.test(String(period))) {
      return res.status(400).json({ ok: false, error: "Periodo inválido. Use YYYY-MM" });
    }

    const [yy, mm] = String(period).split("-").map(Number);

    // OJO: Exportamos desde la fuente real del mes
    // Ajusta si tu sistema usa otra tabla como fuente (pero por lo que cuentas, debe ser full_sales).
    const { rows } = await pool.query(
      `
      SELECT
        estado_liquidacion,
        linea_negocio,
        cuenta,
        ot,
        idasesor,
        nombreasesor,
        cantserv,
        tipored,
        division,
        area,
        zona,
        poblacion,
        d_distrito,
        renta,
        fecha,
        venta,
        tipo_registro,
        estrato,
        paquete_pvd,
        mintic,
        tipo_prodcuto,
        ventaconvergente,
        venta_instale_dth,
        sac_final,
        cedula_vendedor,
        nombre_vendedor,
        modalidad_venta,
        tipo_vendedor,
        tipo_red_comercial,
        nombre_regional,
        nombre_comercial,
        nombre_lider,
        retencion_control,
        observ_retencion,
        tipo_contrato,
        tarifa_venta,
        comision_neta,
        punto_equilibrio
      FROM siapp.full_sales
      WHERE period_year = $1
        AND period_month = $2
      ORDER BY fecha NULLS LAST, idasesor NULLS LAST, ot NULLS LAST
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

    ws.addTable({
      name: "TablaSiapp",
      ref: "A1",
      headerRow: true,
      style: {
        theme: "TableStyleMedium2",
        showRowStripes: true
      },
      columns: headers.map((h) => ({ name: h })),
      rows: rows.map((r) => ([
        r.estado_liquidacion,
        r.linea_negocio,
        r.cuenta,
        r.ot,
        r.idasesor,
        r.nombreasesor,
        r.cantserv != null ? Number(r.cantserv) : null,
        r.tipored,
        r.division,
        r.area,
        r.zona,
        r.poblacion,
        r.d_distrito,
        r.renta != null ? Number(r.renta) : null,
        r.fecha, // si es date/timestamp en DB, ExcelJS lo respeta como Date si viene como Date
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
        r.tarifa_venta != null ? Number(r.tarifa_venta) : null,
        r.comision_neta != null ? Number(r.comision_neta) : null,
        r.punto_equilibrio != null ? Number(r.punto_equilibrio) : null
      ]))
    });

    styleHeader(ws);
    autoFitColumns(ws);
    applyFullBorders(ws, ws.rowCount);

    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="SIAPP-${period}.xlsx"`
    );

    await wb.xlsx.write(res);
    res.end();
  } catch (e) {
    console.error("EXPORT SIAPP ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
}

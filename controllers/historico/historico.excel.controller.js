import ExcelJS from "exceljs";
import pool from "../../config/database.js";

// ======================================================
// Helpers comunes (mismo estilo que export SIAPP)
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

function applyFullBorders(ws) {
  ws.eachRow({ includeEmpty: true }, (row) => {
    row.eachCell({ includeEmpty: true }, (cell) => {
      cell.border = {
        top: { style: "thin" },
        left: { style: "thin" },
        bottom: { style: "thin" },
        right: { style: "thin" }
      };
    });
  });
}

function autoFitColumns(ws) {
  ws.columns.forEach((col) => {
    let max = 10;
    col.eachCell({ includeEmpty: true }, (cell) => {
      const val = cell.value ? String(cell.value) : "";
      max = Math.max(max, val.length + 2);
    });
    col.width = max;
  });
}

// ======================================================
// EXPORTAR BACKUP SIAPP COMPLETO A EXCEL
// ======================================================
export async function exportHistoricoSiappExcel(req, res) {
  try {
    const { periodo_backup } = req.params;

    if (!periodo_backup) {
      return res.status(400).json({ ok: false, error: "periodo_backup requerido" });
    }

    // ---------------------------------------------
    // 1. Traer el backup completo
    // ---------------------------------------------
    const { rows } = await pool.query(
      `
      SELECT *
      FROM historico.siapp_full_backup
      WHERE periodo_backup = $1
      ORDER BY id
      `,
      [periodo_backup]
    );

    if (!rows.length) {
      return res.status(404).json({ ok: false, error: "Backup no encontrado" });
    }

    // ---------------------------------------------
    // 2. Crear Excel
    // ---------------------------------------------
    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet("Backup SIAPP");

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

    // ---------------------------------------------
    // 3. Cargar las filas en el orden correcto
    // ---------------------------------------------
    for (const r of rows) {
      ws.addRow([
        r.estado_liquidacion,
        r.linea_negocio,
        r.cuenta,
        r.ot,
        r.idasesor,
        r.nombreasesor,
        r.cantserv,
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
    applyFullBorders(ws);
    autoFitColumns(ws);

    // ---------------------------------------------
    // 4. Descargar archivo
    // ---------------------------------------------
    res.setHeader("Content-Type", "application/vnd.openxmlformats");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="Backup-SIAPP-${periodo_backup}.xlsx"`
    );

    await wb.xlsx.write(res);
    res.end();

  } catch (e) {
    console.error("[EXPORT HISTORICO SIAPP ERROR]", e);
    res.status(500).json({ ok: false, error: e.message });
  }
}




// ======================================================================
//  EXPORTAR HISTÓRICO — PRESUPUESTO JERARQUÍA (Excel XLSX)
// ======================================================================

// ======================================================
// Helpers de estilo (idénticos a SIAPP y Nómina)
// ======================================================
function styleHeader2(ws) {
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

function applyFullBorders2(ws) {
  ws.eachRow({ includeEmpty: true }, (row) => {
    row.eachCell({ includeEmpty: true }, (cell) => {
      cell.border = {
        top: { style: "thin" },
        left: { style: "thin" },
        bottom: { style: "thin" },
        right: { style: "thin" }
      };
    });
  });
}

function autoFitColumns2(ws) {
  ws.columns.forEach((col) => {
    if (col.width) return;
    let max = 12;
    col.eachCell({ includeEmpty: true }, (cell) => {
      const v = cell.value ? String(cell.value) : "";
      max = Math.max(max, v.length + 2);
    });
    col.width = max;
  });
}

// ======================================================================
//  EXPORTAR HISTÓRICO DE PRESUPUESTO JERARQUÍA
// ======================================================================
export async function exportHistoricoPresupuesto(req, res) {
  try {
    const periodo = req.params.periodo;

    // --------------------------------------------
    // 1) Obtener todos los registros del periodo
    // --------------------------------------------
    const { rows } = await pool.query(
      `
      SELECT 
        (data->>'cedula') AS cedula,
        (data->>'nivel') AS nivel,
        (data->>'nombre') AS nombre,
        (data->>'cargo') AS cargo,
        (data->>'distrito') AS distrito,
        (data->>'regional') AS regional,
        (data->>'presupuesto') AS presupuesto,
        (data->>'telefono') AS telefono,
        (data->>'correo') AS correo,
        (data->>'capacidad') AS capacidad,
        (data->>'loaded_at') AS loaded_at
      FROM historico.presupuesto_jerarquia_backup
      WHERE periodo = $1
      ORDER BY (data->>'cedula')
      `,
      [periodo]
    );

    if (!rows.length) {
      return res.status(404).json({
        ok: false,
        error: "No existe backup para este periodo"
      });
    }

    // --------------------------------------------
    // 2) Crear archivo Excel
    // --------------------------------------------
    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet("Presupuesto_Jerarquia");

    // --------------------------------------------
    // 3) Encabezados en orden del archivo original
    // --------------------------------------------
    const headers = [
      "Cédula",
      "Nivel",
      "Nombre",
      "Cargo",
      "Distrito",
      "Regional",
      "Presupuesto",
      "Teléfono",
      "Correo",
      "Capacidad",
      "Fecha_Cargue"
    ];

    ws.addRow(headers);

    // --------------------------------------------
    // 4) Agregar filas
    // --------------------------------------------
    for (const r of rows) {
      ws.addRow([
        r.cedula,
        r.nivel,
        r.nombre,
        r.cargo,
        r.distrito,
        r.regional,
        r.presupuesto ? Number(r.presupuesto) : null,
        r.telefono,
        r.correo,
        r.capacidad ? Number(r.capacidad) : null,
        r.loaded_at
      ]);
    }

    // --------------------------------------------
    // 5) Estilos
    // --------------------------------------------
    styleHeader2(ws);
    applyFullBorders2(ws);
    autoFitColumns2(ws);

    // --------------------------------------------
    // 6) Exportar archivo
    // --------------------------------------------
    res.setHeader("Content-Type", "application/vnd.openxmlformats");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="Presupuesto_Jerarquia_${periodo}.xlsx"`
    );

    await wb.xlsx.write(res);
    res.end();

  } catch (e) {
    console.error("EXPORT HIST PRESP ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
}

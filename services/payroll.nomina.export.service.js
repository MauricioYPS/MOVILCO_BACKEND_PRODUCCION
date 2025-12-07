// services/payroll.nomina.export.service.js
import ExcelJS from 'exceljs';
import { calculateKpiForPeriod } from './kpi.calculate.service.js';

/**
 * Genera archivo nómina consolidado en Excel en UNA sola hoja.
 */
export async function exportNominaExcel({ period }) {
  if (!period) throw new Error("Falta parámetro period=YYYY-MM");

  // 1. Usamos el KPI consolidado como DATA SOURCE
  const kpi = await calculateKpiForPeriod(period);

  const workbook = new ExcelJS.Workbook();
  const ws = workbook.addWorksheet('NOMINA', {
    properties: { defaultRowHeight: 20 }
  });

  // 🎨 Estilos básicos
  function bold(cell) {
    ws.getCell(cell).font = { bold: true };
  }

  // 2. Encabezados EXACTOS del archivo Nómina que definimos
  const headers = [
    'ITEM',
    'CEDULA',
    'NOMBRE DE FUNCIONARIO',
    'CONTRATADO',
    'DISTRITO',
    'DISTRITO CLARO',
    'FECHA INICIO CONTRATO',
    'FECHA FIN CONTRATO',
    'NOVEDADES',
    'ESTADO',
    'PRESUPUESTO MES',
    'DIAS LABORADOS AL 31',
    'PRORRATEO SEGÚN NOVEDADES',
    'RECREO - DIAS LAB. AL 31',
    'GARANTIZADO PARA COMISIONAR',
    `GARANTIZADO AL ${period} (CON NOVEDADES)`,
    'VENTAS EN DISTRITO',
    'VENTAS FUERA DEL DISTRITO',
    'TOTAL VENTAS',
    'DIFERENCIA EN DISTRITO',
    'DIFERENCIA TOTAL',
    'SI CUMPLE DISTRITO ZONIFICADO',
    'SI CUMPLE / NO CUMPLE'
  ];

  ws.addRow(headers);
  const headerRow = ws.getRow(1);
  headerRow.font = { bold: true };
  headerRow.alignment = { vertical: 'middle', horizontal: 'center' };

  // Ajustar ancho columnas automáticamente
  ws.columns = headers.map(() => ({ width: 25 }));

  // 3. INSERTAR FILAS
  let index = 1;

  for (const row of kpi.detalle) {
    ws.addRow([
      index++,
      row.cedula,
      row.nombre,
      row.contratado_si_no,
      row.distrito,
      row.distrito_claro,
      row.fecha_inicio_contrato ?? '',
      row.fecha_fin_contrato ?? '',
      row.novedades ?? '',
      row.estado,
      row.presupuesto_mes,
      row.dias_laborados_31,
      row.prorrateo_novedades,
      row.dias_laborados_31, // recreo usa lo mismo
      row.prorrateo_novedades, // garantizado para comisionar
      row.prorrateo_novedades, // garantizado con novedades
      row.ventas_distrito,
      row.ventas_fuera_distrito,
      row.total_ventas,
      row.diferencia_en_distrito,
      row.diferencia_total,
      row.cumple_distrito_zonificado,
      row.cumple_global
    ]);
  }

  // 4. Exportar a buffer
  const buffer = await workbook.xlsx.writeBuffer();

  return {
    mime: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    filename: `nomina_${period}.xlsx`,
    buffer
  };
}

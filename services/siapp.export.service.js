// services/siapp.export.service.js
import pool from "../config/database.js";

export async function exportOneSaleToSiapp(rawSale) {
  const now = new Date();

  // LISTA REAL de columnas en siapp.generated_sales
  const validCols = {
    asesor_id: true,
    coordinator_id: true,
    fecha: true,
    estado_liquidacion: true,
    linea_negocio: true,
    cuenta: true,
    ot: true,
    idasesor: true,
    nombreasesor: true,
    cantserv: true,
    tipored: true,
    division: true,
    area: true,
    zona: true,
    poblacion: true,
    d_distrito: true,
    renta: true,
    venta: true,
    tipo_registro: true,
    estrato: true,
    paquete_pvd: true,
    mintic: true,
    tipo_prodcuto: true, // typo oficial
    ventaconvergente: true,
    venta_instale_dth: true,
    sac_final: true,
    cedula_vendedor: true,
    nombre_vendedor: true,
    modalidad_venta: true,
    tipo_vendedor: true,
    tipo_red_comercial: true,
    nombre_regional: true,
    nombre_comercial: true,
    nombre_lider: true,
    retencion_control: true,
    observ_retencion: true,
    tipo_contrato: true,
    tarifa_venta: true,
    comision_neta: true,
    punto_equilibrio: true,
    coordinator_sale_id: true,
    raw_sale_id: true,
    normalized_district: true,
    period_year: true,
    period_month: true,
    is_official: true,
    validated: true,
    source: true,
    source_file_name: true,
    created_at: true,
    updated_at: true
  };

  const payload = {};

  // Copia solo columnas válidas
  for (const [key, value] of Object.entries(rawSale)) {
    if (validCols[key]) payload[key] = value;
  }

  // Correcciones obligatorias
  payload.idasesor = String(rawSale.asesor_id ?? rawSale.idasesor ?? "");
  payload.venta = String(rawSale.venta ?? "1");

  const fecha = rawSale.fecha ? new Date(rawSale.fecha) : now;
  payload.period_year = fecha.getFullYear();
  payload.period_month = fecha.getMonth() + 1;

  payload.normalized_district = rawSale.d_distrito ?? null;
  
  payload.source = "manual";
  payload.source_file_name = "workflow-export";
  payload.is_official = false;
  payload.validated = false;

  payload.created_at = now;
  payload.updated_at = now;

  // Inserción dinámica
  const keys = Object.keys(payload);
  const values = Object.values(payload);
  const cols = keys.join(",");
  const placeholders = keys.map((_, i) => `$${i + 1}`).join(",");

  const q = `
    INSERT INTO siapp.generated_sales (${cols})
    VALUES (${placeholders})
    RETURNING *;
  `;

  const { rows } = await pool.query(q, values);
  return rows[0];
}

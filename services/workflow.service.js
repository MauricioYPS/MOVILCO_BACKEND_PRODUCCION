import pool from "../config/database.js";

/* ============================================================
   1) OBTENER RAW
   ============================================================ */
export async function getRawSaleById(id) {
  const q = `SELECT * FROM kpi.ventas_asesor_raw WHERE id = $1`;
  const { rows } = await pool.query(q, [id]);
  return rows[0] || null;
}

/* ============================================================
   2) APROBAR RAW
   ============================================================ */
export async function approveRawSale(id) {
  const q = `
    UPDATE kpi.ventas_asesor_raw
    SET estado_revision = 'aprobado', updated_at = NOW()
    WHERE id = $1
    RETURNING *;
  `;
  const { rows } = await pool.query(q, [id]);
  return rows[0];
}

/* ============================================================
   3) INSERTAR EN ventas_coordinador
   ============================================================ */
export async function insertCoordinatorSale(raw) {
  const now = new Date();

  // Reforzar identidad asesor
  const uQ = `SELECT id, document_id, name FROM core.users WHERE id = $1`;
  const { rows } = await pool.query(uQ, [raw.asesor_id]);
  const asesor = rows[0];
  if (!asesor) throw new Error("Asesor no encontrado");

  const payload = {
    ...raw,
    idasesor: asesor.document_id,
    nombreasesor: asesor.name,
    ready_for_export: false,
    created_at: now,
    updated_at: now,
  };

  const keys = Object.keys(payload);
  const values = Object.values(payload);
  const cols = keys.join(",");
  const placeholders = keys.map((_, i) => `$${i + 1}`).join(",");

  const q = `
    INSERT INTO kpi.ventas_coordinador (${cols})
    VALUES (${placeholders})
    RETURNING *;
  `;

  const { rows: inserted } = await pool.query(q, values);
  return inserted[0];
}

/* ============================================================
   4) EXPORTAR A siapp.generated_sales
   ============================================================ */
export async function insertIntoGeneratedSales(sale) {
  const now = new Date();

  // Obtener identidad real del asesor SIEMPRE
  const qU = `SELECT id, document_id, name FROM core.users WHERE id = $1`;
  const { rows: userRows } = await pool.query(qU, [sale.asesor_id]);
  const asesor = userRows[0];

  if (!asesor) throw new Error("Asesor no existe");

  const payload = {
    asesor_id: asesor.id,
    idasesor: asesor.document_id,  // ← CRÍTICO
    nombreasesor: asesor.name,     // ← CRÍTICO
  };

  // columnas válidas según generated_sales
  const allowed = {
    coordinator_id: true,
    fecha: true,
    estado_liquidacion: true,
    linea_negocio: true,
    cuenta: true,
    ot: true,
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
    tipo_prodcuto: true, // nombre real SIAPP
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
    punto_equilibrio: true
  };

  for (const [key, val] of Object.entries(sale)) {

    if (!allowed[key]) continue;

    // Mapeo tipo_producto → tipo_prodcuto (por si vino RAW)
    if (key === "tipo_producto") {
      payload.tipo_prodcuto = val;
      continue;
    }

    payload[key] = val;
  }

  // venta siempre texto
  payload.venta = String(payload.venta ?? "1");

  // Fecha + periodo
  const fecha = sale.fecha ? new Date(sale.fecha) : now;
  payload.fecha = fecha;
  payload.period_year = fecha.getFullYear();
  payload.period_month = fecha.getMonth() + 1;

  payload.normalized_district = sale.d_distrito ?? null;

  // metadata
  payload.source = "manual";
  payload.source_file_name = "workflow-export";
  payload.created_at = now;
  payload.updated_at = now;
  payload.validated = false;
  payload.is_official = false;

  // Inserción
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

/* ============================================================
   5) MARCAR COMO EXPORTADA
   ============================================================ */
export async function markCoordinatorSaleExported(id) {
  const q = `
    UPDATE kpi.ventas_coordinador
    SET ready_for_export = TRUE
    WHERE id = $1
    RETURNING *;
  `;
  const { rows } = await pool.query(q, [id]);
  return rows[0];
}

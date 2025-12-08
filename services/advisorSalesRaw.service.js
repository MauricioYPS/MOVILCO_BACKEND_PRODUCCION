import pool from "../config/database.js";

/**
 * Crear una venta RAW (registrada por el asesor)
 * Forzamos:
 *  - idasesor = document_id del asesor real
 *  - nombreasesor = name del asesor real
 */
export async function createAdvisorRawSale(data) {

  // Obtener datos reales del asesor desde core.users
  const qUser = `SELECT id, document_id, name FROM core.users WHERE id = $1`;
  const { rows: userRows } = await pool.query(qUser, [data.asesor_id]);
  const asesor = userRows[0];

  if (!asesor) throw new Error("Asesor no existe en core.users");

  const allowed = {
    // IDENTIDAD DEL ASESOR SE IGNORA → siempre reemplazada
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
    tipo_producto: true,   // nombre correcto RAW
    venta_convergente: true,
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

  const payload = {
    asesor_id: asesor.id,
    idasesor: asesor.document_id,
    nombreasesor: asesor.name,
  };

  // Copiar campos permitidos
  for (const [key, val] of Object.entries(data)) {
    if (!allowed[key]) continue;

    if (key === "venta_convergente") {
      payload.venta_convergente = val;
      continue;
    }

    payload[key] = val;
  }

  payload.venta = String(payload.venta ?? "1");

  payload.estado_revision = "pendiente";
  payload.created_at = new Date();
  payload.updated_at = new Date();

  const keys = Object.keys(payload);
  const values = Object.values(payload);

  const cols = keys.map(k => `"${k}"`).join(",");
  const placeholders = keys.map((_, i) => `$${i + 1}`).join(",");

  const query = `
    INSERT INTO kpi.ventas_asesor_raw (${cols})
    VALUES (${placeholders})
    RETURNING *;
  `;

  const { rows } = await pool.query(query, values);
  return rows[0];
}


/**
 * Obtener ventas RAW
 */
export async function getAdvisorRawSales({ advisorId, month }) {
  let params = [advisorId];
  let where = `WHERE asesor_id = $1`;

  if (month) {
    params.push(month);
    where += ` AND TO_CHAR(fecha, 'YYYY-MM') = $2`;
  }

  const query = `
    SELECT *
    FROM kpi.ventas_asesor_raw
    ${where}
    ORDER BY fecha DESC;
  `;

  const { rows } = await pool.query(query, params);
  return rows;
}


/**
 * Actualizar venta RAW
 */
export async function updateAdvisorRawSale(id, data) {
  const keys = Object.keys(data);
  const values = Object.values(data);

  const setClause = keys.map((k, i) => `${k} = $${i + 1}`).join(",");

  const query = `
    UPDATE kpi.ventas_asesor_raw
    SET ${setClause}, updated_at = NOW()
    WHERE id = $${keys.length + 1}
    RETURNING *;
  `;

  values.push(id);

  const { rows } = await pool.query(query, values);
  return rows[0];
}


/**
 * Eliminar RAW
 */
export async function deleteAdvisorRawSale(id) {
  await pool.query(`DELETE FROM kpi.ventas_asesor_raw WHERE id = $1`, [id]);
  return true;
}


/**
 * Pendientes del coordinador
 */
export async function getPendingAdvisorRawSales(coordinatorId) {
  const query = `
    SELECT *
    FROM kpi.ventas_asesor_raw
    WHERE coordinator_id = $1
      AND estado_revision = 'pendiente'
    ORDER BY fecha DESC;
  `;

  const { rows } = await pool.query(query, [coordinatorId]);
  return rows;
}


/**
 * Cambiar estado
 */
export async function setAdvisorRawSaleStatus(id, status) {
  const query = `
    UPDATE kpi.ventas_asesor_raw
    SET estado_revision = $1, updated_at = NOW()
    WHERE id = $2
    RETURNING *;
  `;

  const { rows } = await pool.query(query, [status, id]);
  return rows[0];
}

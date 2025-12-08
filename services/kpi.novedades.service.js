import pool from "../config/database.js";

export async function createNovedad({ user_id, tipo, fecha_inicio, fecha_fin, descripcion }) {
  const q = `
    INSERT INTO kpi.novedades (user_id, tipo, fecha_inicio, fecha_fin, descripcion)
    VALUES ($1, $2, $3, $4, $5)
    RETURNING *
  `;
  const { rows } = await pool.query(q, [
    user_id,
    tipo,
    fecha_inicio,
    fecha_fin,
    descripcion || null
  ]);
  return rows[0];
}


export async function getNovedadesFiltered({ user_id, month }) {

  let filters = [];
  let params = [];
  let idx = 1;

  // 1) Filtro por usuario
  if (user_id) {
    filters.push(`user_id = $${idx++}`);
    params.push(Number(user_id));
  }

  // 2) Filtro por mes YYYY-MM
  if (month) {
    const m = String(month).match(/^(\d{4})-(\d{2})$/);

    if (m) {
      const year = Number(m[1]);
      const mon  = Number(m[2]);

      // Primer día del mes
      const monthStart = `${year}-${String(mon).padStart(2, "0")}-01`;

      // Condición de solapamiento entre:
      // [fecha_inicio, fecha_fin]  y  [monthStart, monthStart+1mes-1día]
      filters.push(`
        (
          fecha_inicio <= ($${idx}::date + INTERVAL '1 month - 1 day')
          AND fecha_fin   >= $${idx}::date
        )
      `);

      params.push(monthStart);
      idx++;
    }
  }

  const where = filters.length ? `WHERE ${filters.join(" AND ")}` : "";

  const q = `
    SELECT *
    FROM kpi.novedades
    ${where}
    ORDER BY fecha_inicio ASC
  `;

  const { rows } = await pool.query(q, params);
  return rows;
}


export async function deleteNovedad(id) {
  await pool.query(`DELETE FROM kpi.novedades WHERE id = $1`, [id]);
}

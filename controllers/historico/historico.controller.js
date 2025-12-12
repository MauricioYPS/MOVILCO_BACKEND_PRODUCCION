// ======================================================================
// CONTROLADOR — HISTÓRICO SIAPP (VERSIÓN FINAL 2025-12-12)
// Adaptado al nuevo modelo:
//   - backup_key  (clave única con timestamp)
//   - periodo_comercial (YYYY-MM del archivo SIAPP)
//   - sin columna "periodo"
// ======================================================================

import pool from "../../config/database.js";

// ----------------------------------------------------------
// 1) LISTAR BACKUPS DISPONIBLES
// ----------------------------------------------------------
// ======================================================================
// LISTAR BACKUPS — AGRUPADOS POR EJECUCIÓN DE PROMOTE
// ======================================================================

// ===========================================================
// LISTAR BACKUPS SIAPP (agrupados por promote / backup_key)
// GET /api/historico/siapp
// ===========================================================

export async function listHistoricoSiapp(req, res) {
  try {
    const { rows } = await pool.query(`
      SELECT 
        periodo_comercial,
        periodo_backup,
        created_at
      FROM historico.siapp_full_backup
      GROUP BY periodo_comercial, periodo_backup, created_at
      ORDER BY created_at DESC
    `);

    return res.json({
      ok: true,
      backups: rows
    });

  } catch (e) {
    console.error("LIST HISTORICO SIAPP ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
}





// ----------------------------------------------------------
// 2) OBTENER BACKUP POR KEY
// ----------------------------------------------------------
export async function getHistoricoSiapp(req, res) {
  try {
    const backup_key = req.params.key;

    const { rows } = await pool.query(`
      SELECT *
      FROM historico.siapp_full_backup
      WHERE backup_key = $1
    `, [backup_key]);

    if (!rows.length) {
      return res.status(404).json({
        ok: false,
        error: "Backup no encontrado"
      });
    }

    return res.json({
      ok: true,
      backup_key,
      registros: rows
    });

  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
}

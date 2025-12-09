/*******************************************************
 * USER PROFILE CONTROLLER — Asesor + Coordinador
 *******************************************************/
import pool from "../../config/database.js";

export async function getUserFullProfile(req, res) {
    try {
        const userId = Number(req.params.id);

        if (!userId) {
            return res.status(400).json({ ok: false, error: "ID inválido" });
        }

        /********************************************
         * 1) Datos completos del ASESOR
         ********************************************/
        const asesorQuery = `
            SELECT *
            FROM core.users
            WHERE id = $1
        `;
        const asesorRes = await pool.query(asesorQuery, [userId]);
        const asesor = asesorRes.rows[0];

        if (!asesor) {
            return res.status(404).json({ ok: false, error: "Asesor no encontrado" });
        }

        /********************************************
         * 2) Unidad organizativa del asesor
         ********************************************/
        const orgUnitQuery = `
            SELECT id, name, unit_type, parent_id
            FROM core.org_units
            WHERE id = $1
        `;
        const orgUnit = (await pool.query(orgUnitQuery, [asesor.org_unit_id])).rows[0];

        if (!orgUnit) {
            return res.json({
                ok: true,
                asesor,
                coordinador: null,
                direccion: null,
                gerencia: null
            });
        }

        /********************************************
         * 3) OBtener Dirección (padre de la coordinación)
         ********************************************/
        const direccion = orgUnit.parent_id
            ? (await pool.query(orgUnitQuery, [orgUnit.parent_id])).rows[0]
            : null;

        /********************************************
         * 4) Obtener GERENCIA (padre de la dirección)
         ********************************************/
        let gerencia = null;
        if (direccion?.parent_id) {
            gerencia = (await pool.query(orgUnitQuery, [direccion.parent_id])).rows[0];
        }

        /********************************************
         * 5) Obtener COORDINADOR (usuario dentro de la coordinación)
         ********************************************/
        let coordinador = null;

        if (direccion) {
            const coordQuery = `
    SELECT id, name, email, phone, document_id, district, district_claro, cargo
    FROM core.users
    WHERE org_unit_id = $1
      AND (
            cargo ILIKE '%COORDINADOR%' OR
            role = 'COORDINACION' OR
            jerarquia = 'COORDINACION'
          )
    LIMIT 1
`;

            const coordRes = await pool.query(coordQuery, [orgUnit.id]);
            coordinador = coordRes.rows[0] || null;
        }

        /********************************************
         * 6) Respuesta final
         ********************************************/
        return res.json({
            ok: true,
            asesor,
            organizacion: {
                coordinacion: orgUnit,
                direccion,
                gerencia
            },
            coordinador
        });

    } catch (err) {
        console.error("[getUserFullProfile] error:", err);
        return res.status(500).json({
            ok: false,
            error: "Error interno del servidor",
            detail: err.message
        });
    }
}

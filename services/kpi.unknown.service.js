import pool from "../config/database.js";
import { parseSiappDate } from "../utils/parse-date-siapp.js";

/**
 * Obtiene ventas de asesores NO registrados en core.users,
 * con TODAS las columnas extendidas almacenadas en
 * kpi.asesores_fuera_presupuesto.
 *
 * - period=YYYY-MM   → obligatorio
 * - detalle=true     → incluye ventas_detalle completas
 * - fecha=YYYY-MM    → filtra por mes real de SIAPP
 * - documento=XXXXX  → filtra por cédula/idasesor
 */
export async function getUnknownAdvisors(period, detalle = false, fechaFiltro = null, documento = null) {
    if (!period) throw new Error("Falta ?period=YYYY-MM");

    const m = String(period).match(/^(\d{4})-(\d{2})$/);
    if (!m) throw new Error("Periodo inválido. Usa YYYY-MM");

    const year = Number(m[1]);
    const month = Number(m[2]);

    // ------------------------------------------------------------------
    // 1. Cargar todas las ventas desconocidas del periodo
    // ------------------------------------------------------------------
    let { rows } = await pool.query(
        `
    SELECT *
    FROM kpi.asesores_fuera_presupuesto
    WHERE period_year = $1 AND period_month = $2
    ORDER BY fecha ASC, id ASC
    `,
        [year, month]
    );

    // ------------------------------------------------------------------
    // 2. FILTRO POR DOCUMENTO
    // ------------------------------------------------------------------
    if (documento) {
        const doc = String(documento).trim();
        rows = rows.filter((r) => String(r.idasesor || "").trim() === doc);
    }

    // ------------------------------------------------------------------
    // 3. Preparar filtro por fecha real del SIAPP
    // ------------------------------------------------------------------
    let filtroMes = null;

    if (fechaFiltro) {
        const fx = String(fechaFiltro).match(/^(\d{4})-(\d{2})$/);
        if (fx) filtroMes = { y: Number(fx[1]), m: Number(fx[2]) };
    }

    // ------------------------------------------------------------------
    // 4. Agrupar ventas por asesor
    // ------------------------------------------------------------------
    const map = {};

    for (const r of rows) {
        const ced = String(r.idasesor || "").trim();
        if (!ced) continue;

        if (!map[ced]) {
            map[ced] = {
                idasesor: ced,
                nombreasesor: r.nombreasesor || null,
                ventas_total: 0,
                ventas_detalle: []
            };
        }

        // --------------------------------------------------------------
        // 5. Filtro por fecha real
        // --------------------------------------------------------------
        let incluir = true;

        if (filtroMes) {
            const parsed = parseSiappDate(r.fecha);

            if (!parsed.date) {
                incluir = true;
                r.advertencia = "Fecha inválida en SIAPP";
            } else {
                const fy = parsed.date.getFullYear();
                const fm = parsed.date.getMonth() + 1;
                incluir = fy === filtroMes.y && fm === filtroMes.m;
            }
        }

        if (!incluir) continue;

        map[ced].ventas_total++;

        if (!detalle) continue;

        // --------------------------------------------------------------
        // 6. Añadir TODAS las columnas de la tabla tal cual
        // --------------------------------------------------------------
        const ventaNum = map[ced].ventas_detalle.length + 1;

        map[ced].ventas_detalle.push({
            venta_num: ventaNum,

            id: r.id,
            period_year: r.period_year,
            period_month: r.period_month,
            estado_liquidacion: r.estado_liquidacion,
            linea_negocio: r.linea_negocio,
            cuenta: r.cuenta,
            ot: r.ot,
            idasesor: r.idasesor,
            nombreasesor: r.nombreasesor,
            cantserv: r.cantserv,
            tipored: r.tipored,
            division: r.division,
            area: r.area,
            zona: r.zona,
            poblacion: r.poblacion,
            d_distrito: r.d_distrito,
            renta: r.renta,
            fecha: r.fecha,
            venta: r.venta,
            tipo_registro: r.tipo_registro,
            estrato: r.estrato,
            paquete_pvd: r.paquete_pvd,
            mintic: r.mintic,
            tipo_prodcuto: r.tipo_prodcuto,
            ventaconvergente: r.ventaconvergente,
            venta_instale_dth: r.venta_instale_dth,
            sac_final: r.sac_final,
            cedula_vendedor: r.cedula_vendedor,
            nombre_vendedor: r.nombre_vendedor,
            modalidad_venta: r.modalidad_venta,
            tipo_vendedor: r.tipo_vendedor,
            tipo_red_comercial: r.tipo_red_comercial,
            nombre_regional: r.nombre_regional,
            nombre_comercial: r.nombre_comercial,
            nombre_lider: r.nombre_lider,
            retencion_control: r.retencion_control,
            observ_retencion: r.observ_retencion,
            tipo_contrato: r.tipo_contrato,
            tarifa_venta: r.tarifa_venta,
            comision_neta: r.comision_neta,
            punto_equilibrio: r.punto_equilibrio
        });

    }

    // ------------------------------------------------------------------
    // 7. Convertir a arreglo y ordenar por número total de ventas
    // ------------------------------------------------------------------
    const result = Object.values(map).sort((a, b) => b.ventas_total - a.ventas_total);

    return {
        ok: true,
        period: `${year}-${String(month).padStart(2, "0")}`,
        total_asesores: result.length,
        total_ventas: result.reduce((a, b) => a + b.ventas_total, 0),
        detalle,
        data: result
    };
}

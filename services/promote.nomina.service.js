// services/promote.nomina.service.js
import pool from "../config/database.js";

function norm(str) {
  return String(str ?? "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

// true/false/null
function parseContratado(v) {
  const s = norm(v);
  if (!s) return null;

  const yes = ["si", "s", "yes", "true", "1", "activo", "vigente", "contratado"];
  const no = ["no", "n", "false", "0", "retirado", "inactivo", "no contratado"];

  if (yes.includes(s)) return true;
  if (no.includes(s)) return false;
  return null;
}

// Superposición del contrato con el mes (YYYY, MM)
function overlapsMonth(contract_start, contract_end, year, month) {
  const periodStart = new Date(Date.UTC(year, month - 1, 1));
  const periodEnd = new Date(Date.UTC(year, month, 0));

  const cs = contract_start ? new Date(contract_start) : new Date("1900-01-01T00:00:00Z");
  const ce = contract_end ? new Date(contract_end) : new Date("2999-12-31T00:00:00Z");

  return cs <= periodEnd && ce >= periodStart;
}

async function getUserByDocument(client, document_id) {
  const { rows } = await client.query(
    `SELECT id, email, active, en_presupuesto
     FROM core.users
     WHERE document_id = $1
     LIMIT 1`,
    [document_id]
  );
  return rows[0] || null;
}

// DESHABILITADA A PROPÓSITO: ya NO se crean usuarios desde nómina.
async function createUserFromNomina() {
  throw new Error("createUserFromNomina() está deshabilitada: los usuarios NO se crean desde nómina.");
}

// Nota: la dejamos por compatibilidad histórica, pero NO se usa.
async function updateUserFromNomina() {
  throw new Error("updateUserFromNomina() está deshabilitada: nómina NO debe modificar core.users.");
}

async function upsertUserMonthly(client, userId, year, month, metric) {
  const {
    presupuesto_mes,
    dias_laborados,
    prorrateo,
    estado_envio_presupuesto,
    novedad,
    activo_en_periodo
  } = metric;

  await client.query(
    `INSERT INTO core.user_monthly
      (user_id, period_year, period_month, presupuesto_mes, dias_laborados, prorrateo, estado_envio_presupuesto, novedad, activo_en_periodo)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
     ON CONFLICT (user_id, period_year, period_month)
     DO UPDATE SET
       presupuesto_mes = EXCLUDED.presupuesto_mes,
       dias_laborados  = EXCLUDED.dias_laborados,
       prorrateo       = EXCLUDED.prorrateo,
       estado_envio_presupuesto = EXCLUDED.estado_envio_presupuesto,
       novedad         = EXCLUDED.novedad,
       activo_en_periodo = EXCLUDED.activo_en_periodo,
       updated_at      = now()`,
    [
      userId,
      year,
      month,
      presupuesto_mes ?? null,
      dias_laborados ?? null,
      prorrateo ?? null,
      estado_envio_presupuesto ?? null,
      novedad ?? null,
      (activo_en_periodo === true || activo_en_periodo === false) ? activo_en_periodo : null
    ]
  );
}

export async function promoteNominaFromStaging({ period_year, period_month }) {
  if (!period_year || !period_month) {
    throw new Error("Se requiere period_year y period_month (por ejemplo ?period=2025-10)");
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const { rows } = await client.query(`
      SELECT
        NULLIF(TRIM(cedula), '')                AS document_id,
        NULLIF(TRIM(nombre_funcionario), '')    AS nombre_funcionario,
        NULLIF(TRIM(contratado), '')            AS contratado,
        fecha_inicio_contrato,
        fecha_fin_contrato,
        NULLIF(TRIM(novedad), '')               AS novedad,
        presupuesto_mes,
        dias_laborados,
        prorrateo,
        NULLIF(TRIM(estado_envio_presupuesto), '') AS estado_envio_presupuesto
      FROM staging.archivo_nomina
      ORDER BY cedula ASC NULLS LAST
    `);

    // Contadores
    let created = 0; // debe quedar en 0 siempre
    let updated = 0; // debe quedar en 0 siempre
    let skippedNoDocument = 0;
    let skippedMissingUser = 0;
    let userMonthlyUpserted = 0;

    // Evidencia para auditoría / soporte
    const missing_users = [];

    for (const r of rows) {
      const documentId = r.document_id || null;
      if (!documentId) {
        skippedNoDocument++;
        continue;
      }

      const name = r.nombre_funcionario || "Sin Nombre";
      const contratadoFlag = parseContratado(r.contratado); // true/false/null
      const cStart = r.fecha_inicio_contrato || null;
      const cEnd = r.fecha_fin_contrato || null;

      // Activo mensual (no toca core.users.active)
      const contratadoEff = (contratadoFlag === null) ? true : contratadoFlag;
      const activoEnPeriodo = contratadoEff && overlapsMonth(cStart, cEnd, period_year, period_month);

      // 1) Buscar usuario existente en core.users
      const user = await getUserByDocument(client, documentId);

      // 2) Si NO existe, NO se crea. Se registra y se omite.
      if (!user) {
        skippedMissingUser++;
        missing_users.push({
          document_id: documentId,
          nombre: name,
          motivo: "NO_EXISTE_EN_CORE_USERS (se omite user_monthly; no se crean usuarios desde nómina)"
        });
        continue;
      }

      // 3) Upsert user_monthly SOLO para usuarios existentes
      await upsertUserMonthly(client, user.id, period_year, period_month, {
        presupuesto_mes: r.presupuesto_mes,
        dias_laborados: r.dias_laborados,
        prorrateo: r.prorrateo,
        estado_envio_presupuesto: r.estado_envio_presupuesto,
        novedad: r.novedad,
        activo_en_periodo: activoEnPeriodo
      });

      userMonthlyUpserted++;
    }

    await client.query("COMMIT");

    return {
      created, // 0
      updated, // 0
      user_monthly_upserted: userMonthlyUpserted,
      skippedNoDocument,
      skippedMissingUser,
      missing_users_sample: missing_users.slice(0, 100),
      total: rows.length
    };
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

// services/siapp.monthly-close.service.js
import { promoteNominaFromStaging } from "./promote.nomina.service.js";
import { promoteNovedadesFromStaging } from "./promote.novedades.service.js";
import { promoteSiappFromFullSales } from "./promote.siapp.service.js";

function parsePeriod(period) {
  const p = Array.isArray(period) ? period[0] : period;
  if (!p) return null;

  const m = String(p).trim().match(/^(\d{4})-(\d{1,2})$/);
  if (!m) return null;

  const year = Number(m[1]);
  const month = Number(m[2]);
  if (!Number.isFinite(year) || !Number.isFinite(month)) return null;
  if (month < 1 || month > 12) return null;

  return { year, month };
}

export async function closeMonthlyPeriod({ period }) {
  const per = parsePeriod(period);
  if (!per) throw new Error("Falta o es inválido ?period=YYYY-MM");

  const { year, month } = per;

  // 1) Promover Nómina (crea/actualiza users y crea core.user_monthly del mes)
  const nomina = await promoteNominaFromStaging({
    period_year: year,
    period_month: month
  });

  // 2) Promover Novedades (no depende del periodo en tu implementación actual)
  const novedades = await promoteNovedadesFromStaging();

  // 3) Promover Progress (ventas in/out y métricas por asesor para el mes)
  const progress = await promoteSiappFromFullSales({
    period_year: year,
    period_month: month
  });

  return {
    period: `${year}-${String(month).padStart(2, "0")}`,
    period_year: year,
    period_month: month,
    steps: {
      nomina,
      novedades,
      progress
    }
  };
}

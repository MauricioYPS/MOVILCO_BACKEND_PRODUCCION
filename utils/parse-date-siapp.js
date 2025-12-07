// utils/parse-date-siapp.js

/**
 * Parsea fechas del SIAPP en formato flexible:
 * - D/M/YYYY
 * - DD/MM/YYYY
 * - D/MM/YYYY
 * - DD/M/YYYY
 * - Con o sin ceros a la izquierda
 */
export function parseSiappDate(value) {
  if (!value) return { date: null, invalid: true };

  // Si ya viene como objeto Date
  if (value instanceof Date && !isNaN(value)) {
    return { date: value, invalid: false };
  }

  const str = String(value).trim();

  // Detectar fecha tipo "D/M/YYYY", "DD/MM/YYYY", etc.
  const regex = /^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/;

  const match = str.match(regex);
  if (!match) {
    return { date: null, invalid: true };
  }

  let day = Number(match[1]);
  let month = Number(match[2]); // 1–12
  let year = Number(match[3]);

  // Mes inválido
  if (month < 1 || month > 12) {
    return { date: null, invalid: true };
  }

  const parsed = new Date(year, month - 1, day);

  // Validar fecha real
  if (isNaN(parsed.getTime())) {
    return { date: null, invalid: true };
  }

  return { date: parsed, invalid: false };
}

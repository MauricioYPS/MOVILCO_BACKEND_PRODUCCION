

export function renderTemplate(html = "", data = {}) {
  if (!html) return "";
  if (!data || typeof data !== "object") return html;

  let output = html;

  for (const key of Object.keys(data)) {
    const value = data[key] ?? ""; // soporta null/undefined => ""
    
    // Construye patrón {VARIABLE}
    const regex = new RegExp(`\\{${key}\\}`, "g");

    // Reemplazar todas las ocurrencias
    output = output.replace(regex, value);
  }

  return output;
}

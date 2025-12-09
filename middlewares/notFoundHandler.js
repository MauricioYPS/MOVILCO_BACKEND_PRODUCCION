export function notFoundHandler(req, res) {
  return res.status(404).json({
    ok: false,
    error: "Ruta no encontrada"
  });
}

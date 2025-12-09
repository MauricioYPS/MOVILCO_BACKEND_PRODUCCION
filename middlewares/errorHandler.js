export function errorHandler(err, req, res, next) {
  console.error("[ERROR]", err);

  return res.status(500).json({
    ok: false,
    error: "Error interno del servidor",
    detail: err?.message
  });
}

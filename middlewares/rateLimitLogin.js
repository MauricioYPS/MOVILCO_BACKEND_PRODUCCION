const loginAttempts = new Map();

export function rateLimitLogin(req, res, next) {
  const ip = req.ip;
  const now = Date.now();

  const record = loginAttempts.get(ip) || { attempts: 0, lastTry: now };

  // reset after 5 minutes
  if (now - record.lastTry > 5 * 60 * 1000) {
    record.attempts = 0;
  }

  record.attempts++;
  record.lastTry = now;
  loginAttempts.set(ip, record);

  if (record.attempts > 20) {
    return res.status(429).json({
      ok: false,
      error: "Demasiados intentos, espere 5 minutos"
    });
  }

  next();
}

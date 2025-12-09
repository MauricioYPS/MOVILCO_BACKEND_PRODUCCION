import jwt from "jsonwebtoken";

export function generateToken(payload, expiresIn = 60*60) {
  return jwt.sign(payload, process.env.JWT_SECRET, { expiresIn });
}

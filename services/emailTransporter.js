import nodemailer from "nodemailer";

export const transporter = nodemailer.createTransport({
  host: "smtp.gmail.com",
  port: 587,
  secure: false, 
  auth: {
    user: "yepes060@gmail.com",
    pass: "kgse jexu iboa wabx", 
  },
  tls: {
    rejectUnauthorized: false
  },
  pool: true,           // conexión en pool (mejor rendimiento)
  maxConnections: 5,    // máximo de conexiones simultáneas
  maxMessages: 50,      // máximo de mensajes por conexión
  rateDelta: 2000,      // mínimo 2 segundos entre envíos (Gmail-friendly)
  rateLimit: 1
});

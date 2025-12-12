import nodemailer from "nodemailer";

export const transporter = nodemailer.createTransport({
  host: "smtp.gmail.com",
  port: 587,
  secure: false, 
  auth: {
    user:process.env.TMP_USER,
    pass:process.env.TMP_PASS, 
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
await transporter.sendMail({
  from: `"Test SMTP" <${process.env.MAIL_USER}>`,
  to: "devmauricioy@gmail.com",
  subject: "TEST SMTP MOVILCO",
  html: "<h1>Prueba SMTP</h1>"
})



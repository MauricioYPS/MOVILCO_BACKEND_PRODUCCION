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
  pool: true,           
  maxConnections: 5,    
  maxMessages: 50,     
  rateDelta: 2000,    
  rateLimit: 1
});
// await transporter.sendMail({
//   from: `"Test SMTP" <${process.env.MAIL_USER}>`,
//   to: "devmauricioy@gmail.com",
//   subject: "TEST SMTP MOVILCO",
//   html: "<h1>Prueba SMTP</h1>"
// })



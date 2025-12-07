// config/database.js
import pg from "pg";
import dotenv from "dotenv";

dotenv.config(); // <-- compatible SIEMPRE

const { DATABASE_URL, PGSSLMODE } = process.env;
if (!DATABASE_URL) throw new Error("DATABASE_URL no está definida");
console.log("DEBUG DB URL:", process.env.DATABASE_URL, typeof process.env.DATABASE_URL);

const pool = new pg.Pool({
  connectionString: DATABASE_URL,
  ssl: PGSSLMODE === "disable" ? false : { rejectUnauthorized: false },
  max: 10,
  idleTimeoutMillis: 30000,
});

export default pool;

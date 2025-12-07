import express from 'express'
import 'dotenv/config.js'
import cors from 'cors'
import morgan from 'morgan'
import indexRouter from './router/index.js'
console.log("TYPE OF DATABASE_URL:", typeof process.env.DATABASE_URL);
console.log("RAW DATABASE_URL:", process.env.DATABASE_URL);

console.log("ENV CHECK:", {
  PORT: process.env.PORT,
  DATABASE_URL: process.env.DATABASE_URL,
  JWT_SECRET: process.env.JWT_SECRET
});

const server = express()
const PORT = process.env.PORT
const ready  = () => console.log("Server ready in port :" + PORT);

server.use(express.json())
server.use(express.urlencoded({extended:true}))
server.use(cors())
server.use(morgan('dev'))

server.use('/api',indexRouter)

server.listen(PORT,ready)

import express from 'express'
import 'dotenv/config.js'
import cors from 'cors'
import morgan from 'morgan'
import indexRouter from './router/index.js'
import {errorHandler} from './middlewares/errorHandler.js'
import {notFoundHandler} from './middlewares/notFoundHandler.js'

const server = express()
const PORT = process.env.PORT
const ready  = () => console.log("Server ready in port :" + PORT);

server.use(express.json())
server.use(express.urlencoded({extended:true}))
server.use(
  cors({
    origin: "http://localhost:5173", // tu frontend
    credentials: true, // permite cookies
    methods: ["GET", "POST", "PUT", "DELETE", "PATCH"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);
server.use(morgan('dev'))

server.use('/api',indexRouter)

server.use(notFoundHandler)
server.use(errorHandler)

server.listen(PORT,ready)

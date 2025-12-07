// config/upload.js
// Config simple de multer para guardar archivos subidos en ./uploads
import multer from 'multer'
import fs from 'fs'
import path from 'path'

const UPLOAD_DIR = path.resolve('uploads')
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true })

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, UPLOAD_DIR),
  filename: (req, file, cb) => {
    const timestamp = Date.now()
    // conserva la extensión original
    const original = file.originalname.replace(/\s+/g, '_')
    cb(null, `${timestamp}__${original}`)
  }
})

export const upload = multer({
  storage,
  limits: {
    fileSize: 25 * 1024 * 1024 // 25MB
  },
  fileFilter: (req, file, cb) => {
    // aceptamos xlsx, xls, csv
    const ok = /(excel|spreadsheetml|csv)/i.test(file.mimetype) || /\.(xlsx|xls|csv)$/i.test(file.originalname)
    if (!ok) return cb(new Error('Formato no soportado. Usa .xlsx, .xls o .csv'))
    cb(null, true)
  }
})

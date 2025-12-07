import { resolveStagingTable, listStagingTable } from '../../services/staging.service.js'

export async function listStaging(req, res) {
  try {
    const { dataset } = req.params
    const table = resolveStagingTable(dataset)

    const limit = req.query.limit ?? '50000'
    const offset = req.query.offset ?? '0'

    const result = await listStagingTable(table, { limit, offset })

    res.json({
      dataset,
      table,
      total: result.total,
      limit: result.limit,
      offset: result.offset,
      count: result.rows.length,
      rows: result.rows
    })
  } catch (e) {
    console.error('[STAGING list]', e)
    res.status(500).json({ error: 'No se pudo listar la tabla staging', detail: e.message })
  }
}

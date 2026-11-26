const fs = require('fs')
const { pipeline } = require('stream/promises')
const { Client } = require('pg')
const { to } = require('pg-copy-streams')

async function main() {
  const date = new Date().toISOString().slice(0, 10)
  const file = `/exports/shipments_${date}.csv`

  const client = new Client()
  await client.connect()
  const copy = client.query(to(`COPY (SELECT * FROM shipments WHERE created_at::date = '${date}') TO STDOUT WITH CSV HEADER`))
  await pipeline(copy, fs.createWriteStream(file))
  await client.end()

  console.log(`${copy.rowCount} rows -> ${file}`)
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})

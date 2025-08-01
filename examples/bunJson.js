// Test to make a lot of concurrent writes and verify that they were all written
const N_WRITES = 10

const DB_HOST = "http://localhost:3737"

var successes = 0

async function writeJson(table, data) {
  const writeResp = await fetch(`${DB_HOST}/tables/${table}`, {
    method: "POST",
    body: JSON.stringify(data),
    headers: { "Content-Type": "application/json", "UUID": data["id"] },
  })

  const body = await writeResp.text()

  if (writeResp.status == 200) {
    successes++
  }

  console.log(body, data)
}

async function readAllJson(table) {
  const readResp = await fetch(`${DB_HOST}/tables/${table}`)
  const data = await readResp.text()
  return JSON.parse(data)
}

var records = []

for (let i = 0; i < N_WRITES; i++) {
  const id = Bun.randomUUIDv7()
  const payload = {
    id,
  }
  records.push(payload)
}

const tableName = Bun.randomUUIDv7().substring(30)

console.log("table name:", tableName)

await Promise.all(records.map(record => writeJson(tableName, record)))

console.log("success:", successes)

// await new Promise(r => setTimeout(r, 3000))

// Read all records and assert there are the right amount
const result = await readAllJson(tableName)

console.log(result)

if (result.length != N_WRITES) {
  console.error("wrong number of results", result.length)
}

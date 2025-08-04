const DB_HOST = "http://localhost:3737"

async function writeJson(table, data) {
  const writeResp = await fetch(`${DB_HOST}/tables/${table}`, {
    method: "POST",
    body: JSON.stringify(data),
    headers: { "Content-Type": "application/json", "UUID": data["id"] },
  })

  const body = await writeResp.text()
}

const names = [
  "tim",
  "bill",
  "denise",
  "fievel",
  "traemel",
]

async function writeObject() {
  const payload = {
    id: Bun.randomUUIDv7(),
    name: names[Math.floor(Math.random() * names.length)],
    value: Math.floor(Math.random() * 100000),
  }

  return writeJson("qtest", payload)
}

for (let i = 0; i <= 10000; i++) {
  await writeObject()
}

console.log("done")

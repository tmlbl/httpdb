const DB_HOST = "http://localhost:3737"

async function writeJson(data) {
  const writeResp = await fetch(`${DB_HOST}/tables/querytest`, {
    method: "POST",
    body: JSON.stringify(data),
    headers: { "Content-Type": "application/json", "UUID": data["id"] },
  })

  const body = await writeResp.text()
}

await writeJson({
  id: "tim",
  client: "a",
})

await writeJson({
  id: "john",
  client: "b",
})

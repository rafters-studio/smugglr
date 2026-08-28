// A local HTTP-SQL endpoint in the request and response shape of smugglr's
// `generic` profile: POST {sql, params} in, {columns, rows} out. Backed by
// node:sqlite, so it has no dependencies. It stands in for Cloudflare D1 when
// you have no D1 token; the README's output blocks were captured against it.
//
// Run: node local-endpoint.mjs [db-path] [port]
//
// POST /fail/<n> makes the next <n> queries answer 503, which is how the
// node-auto-sync example shows its backoff.

import { createServer } from "node:http";
import { DatabaseSync } from "node:sqlite";

const dbPath = process.argv[2] ?? "./remote.sqlite";
const db = new DatabaseSync(dbPath);
const port = Number(process.argv[3] ?? 8765);
let failNext = 0;

function query(sql, params) {
  const stmt = db.prepare(sql);
  stmt.setReturnArrays(true);
  const rows = stmt.all(...params);
  return { columns: stmt.columns().map((c) => c.name), rows };
}

const server = createServer(async (req, res) => {
  let body = "";
  for await (const chunk of req) body += chunk;

  if (req.method === "POST" && req.url.startsWith("/fail/")) {
    failNext = Number(req.url.slice("/fail/".length)) || 0;
    res.end(`next ${failNext} queries answer 503\n`);
    return;
  }
  if (failNext > 0) {
    failNext -= 1;
    res.writeHead(503).end("injected outage");
    return;
  }

  try {
    const { sql, params = [] } = JSON.parse(body);
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify(query(sql, params)));
  } catch (err) {
    res.writeHead(400).end(String(err));
  }
});

server.listen(port, "127.0.0.1", () => {
  console.log(`http-sql endpoint on http://127.0.0.1:${port} (db: ${dbPath})`);
});

// Push a local SQLite database to an HTTP-SQL endpoint (Cloudflare D1 in
// production, local-endpoint.mjs on a laptop) with the smugglr npm package.
//
// Run: node --env-file=.env push.mjs

import { readFile } from "node:fs/promises";
import Database from "better-sqlite3";
import { Smugglr, setWasm } from "smugglr";
import * as wasm from "smugglr/wasm";

for (const key of ["DEST_URL", "LOCAL_DB"]) {
  if (!process.env[key]) {
    console.error(`missing env var: ${key}`);
    process.exit(2);
  }
}

// The wasm-bindgen loader fetches the .wasm binary relative to the glue
// module. Node's fetch has no file: scheme, so read the bytes ourselves and
// hand them to the loader before the first init().
const wasmBytes = await readFile(
  new URL("smugglr_wasm_bg.wasm", import.meta.resolve("smugglr/wasm")),
);
await wasm.default({ module_or_path: wasmBytes });
await setWasm(wasm);
console.log(`loaded smugglr wasm: ${wasmBytes.length} bytes`);

const db = new Database(process.env.LOCAL_DB);

// Wrap better-sqlite3 in the SqlExecutor shape smugglr expects: positional
// params, and a {columns, rows} result with rows as arrays.
const executor = {
  async run(sql, params) {
    const stmt = db.prepare(sql);
    if (stmt.reader) {
      return { columns: stmt.columns().map((c) => c.name), rows: stmt.raw().all(params) };
    }
    stmt.run(params);
    return { columns: [], rows: [] };
  },
};

const s = await Smugglr.init({
  source: { type: "local", executor },
  dest: {
    url: process.env.DEST_URL,
    authToken: process.env.DEST_TOKEN,
    profile: process.env.DEST_PROFILE ?? "generic",
  },
  sync: {
    // Setting excludeTables replaces the package defaults, so the migration
    // ledger is listed again alongside D1's own housekeeping tables.
    excludeTables: ["_smugglr_migrations", "_cf_KV", "_cf_METADATA", "d1_migrations"],
    conflictResolution: "local_wins",
  },
});

try {
  const result = await s.push();
  console.log("push complete:", JSON.stringify(result));
} finally {
  s.dispose();
  db.close();
}

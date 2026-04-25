// Push a local SQLite database to Cloudflare D1 using the smugglr npm package.
//
// Run: node --env-file=.env push.mjs

import Database from "better-sqlite3";
import { Smugglr } from "smugglr";

const required = ["CF_ACCOUNT_ID", "CF_API_TOKEN", "CF_D1_DATABASE_ID", "LOCAL_DB"];
for (const key of required) {
  if (!process.env[key]) {
    console.error(`missing env var: ${key}`);
    process.exit(2);
  }
}

const db = new Database(process.env.LOCAL_DB, { readonly: false });

// Wrap better-sqlite3 in the SqlExecutor shape smugglr expects.
const executor = {
  async run(sql, params) {
    const stmt = db.prepare(sql);
    const trimmed = sql.trim().toUpperCase();
    if (trimmed.startsWith("SELECT") || trimmed.startsWith("PRAGMA")) {
      const rows = stmt.raw().all(params);
      const columns = stmt.columns().map((c) => c.name);
      return { columns, rows };
    }
    stmt.run(params);
    return { columns: [], rows: [] };
  },
};

const url = `https://api.cloudflare.com/client/v4/accounts/${process.env.CF_ACCOUNT_ID}/d1/database/${process.env.CF_D1_DATABASE_ID}/query`;

const s = await Smugglr.init({
  source: { type: "local", executor },
  dest: { url, authToken: process.env.CF_API_TOKEN, profile: "d1" },
  sync: {
    excludeTables: ["sqlite_sequence", "_cf_KV", "_cf_METADATA", "d1_migrations"],
    conflictResolution: "local_wins",
  },
});

try {
  const result = await s.push();
  console.log("push complete:", result);
} finally {
  s.dispose();
  db.close();
}

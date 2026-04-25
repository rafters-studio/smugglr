// Long-running sync loop with exponential backoff. Replace with the upcoming
// `autoSync` config option once #113 lands.

import Database from "better-sqlite3";
import { Smugglr, SmugglrError } from "smugglr";

const db = new Database(process.env.LOCAL_DB);
const executor = {
  async run(sql, params) {
    const stmt = db.prepare(sql);
    const trimmed = sql.trim().toUpperCase();
    if (trimmed.startsWith("SELECT") || trimmed.startsWith("PRAGMA")) {
      return {
        columns: stmt.columns().map((c) => c.name),
        rows: stmt.raw().all(params),
      };
    }
    stmt.run(params);
    return { columns: [], rows: [] };
  },
};

const url = `https://api.cloudflare.com/client/v4/accounts/${process.env.CF_ACCOUNT_ID}/d1/database/${process.env.CF_D1_DATABASE_ID}/query`;

const s = await Smugglr.init({
  source: { type: "local", executor },
  dest: { url, authToken: process.env.CF_API_TOKEN, profile: "d1" },
  sync: { conflictResolution: "newer_wins" },
});

const tickInterval = Number(process.env.SYNC_INTERVAL_MS ?? 30_000);
const minBackoff = 1_000;
const maxBackoff = 5 * 60_000;
let backoff = minBackoff;
let inflight = false;
let stopped = false;

function ts() {
  return new Date().toISOString().slice(11, 23);
}

async function tick() {
  if (inflight || stopped) return;
  inflight = true;
  try {
    const result = await s.sync();
    const rows = result.tables.reduce(
      (n, t) => n + (t.rowsPushed ?? 0) + (t.rowsPulled ?? 0),
      0,
    );
    console.log(`[${ts()}] sync ok: ${rows} rows`);
    backoff = minBackoff;
  } catch (err) {
    if (err instanceof SmugglrError && err.retryable) {
      console.warn(`[${ts()}] sync failed (retryable): ${err.message} -- backing off ${backoff}ms`);
      await new Promise((r) => setTimeout(r, backoff));
      backoff = Math.min(backoff * 2, maxBackoff);
    } else {
      console.error(`[${ts()}] sync failed (fatal):`, err);
      stopped = true;
    }
  } finally {
    inflight = false;
  }
}

const handle = setInterval(tick, tickInterval);
tick();

function shutdown(signal) {
  console.log(`[${ts()}] received ${signal}, stopping after current sync...`);
  stopped = true;
  clearInterval(handle);
  const wait = setInterval(() => {
    if (!inflight) {
      clearInterval(wait);
      s.dispose();
      db.close();
      process.exit(0);
    }
  }, 100);
}
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

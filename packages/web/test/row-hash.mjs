// Hashes rows in the browser's SQLite build, with the engine's row hash
// registered on the connection (FR-SMUGGLR-058).
//
// Each argument is one row's single value, written as a SQL literal (the
// value's quote() text). Headless Chromium opens @sqlite.org/sqlite-wasm,
// registers smugglr-web's rowHash as smugglr_row_hash, inserts each literal
// and prints, as JSON, each row's quote() text and its hash in lowercase hex.
// Values travel only as SQL text, never as JS numbers, so none is rounded.
//
// Needs crates/smugglr-web/pkg (`pnpm run build:wasm`) and Playwright's
// Chromium (`pnpm exec playwright install chromium`).

import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const web = fileURLToPath(new URL('..', import.meta.url));
const origin = 'http://smugglr.test';
const served = {
  '/sqlite/': path.join(web, 'node_modules/@sqlite.org/sqlite-wasm/dist'),
  '/engine/': path.join(web, '../../crates/smugglr-web/pkg'),
};
const types = { '.js': 'text/javascript', '.mjs': 'text/javascript', '.wasm': 'application/wasm' };

const literals = process.argv.slice(2);
const browser = await chromium.launch();
try {
  const page = await browser.newPage();
  await page.route(`${origin}/**`, async (route) => {
    const { pathname } = new URL(route.request().url());
    if (pathname === '/') {
      return route.fulfill({ contentType: 'text/html', body: '<!doctype html><title>smugglr</title>' });
    }
    const prefix = Object.keys(served).find((start) => pathname.startsWith(start));
    if (prefix === undefined) {
      return route.fulfill({ status: 404 });
    }
    const file = path.join(served[prefix], pathname.slice(prefix.length));
    try {
      const body = await readFile(file);
      return route.fulfill({ body, contentType: types[path.extname(file)] ?? 'application/octet-stream' });
    } catch {
      return route.fulfill({ status: 404, body: `${file} is missing` });
    }
  });
  await page.goto(`${origin}/`);
  const rows = await page.evaluate(async (literals) => {
    const { default: sqlite3InitModule } = await import('/sqlite/index.mjs');
    const engine = await import('/engine/smugglr_web.js');
    await engine.default();
    const sqlite3 = await sqlite3InitModule();
    const db = new sqlite3.oo1.DB(':memory:');
    db.createFunction({
      name: 'smugglr_row_hash',
      xFunc: (_context, ...columns) => engine.rowHash(columns),
      arity: -1,
      deterministic: true,
    });
    db.exec('CREATE TABLE t (v)');
    for (const literal of literals) {
      db.exec(`INSERT INTO t (v) VALUES (${literal})`);
    }
    return db.selectArrays('SELECT quote(v), lower(hex(smugglr_row_hash(quote(v)))) FROM t ORDER BY rowid');
  }, literals);
  process.stdout.write(`${JSON.stringify(rows)}\n`);
} finally {
  await browser.close();
}

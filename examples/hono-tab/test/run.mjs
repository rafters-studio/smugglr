// Runs the example Worker locally (wrangler dev, with a local D1) and keeps
// tabs built by @smugglr/web, packed from this repository, in step with it
// in headless Chromium. Each check prints one line; any failure exits 1.
//
// Needs vendor/smugglr-web (`pnpm vendor`) and Playwright's Chromium, as
// @smugglr/web's own tests do (`pnpm exec playwright install
// chromium-headless-shell` in packages/web).

import assert from 'node:assert/strict';
import { execFileSync, spawn } from 'node:child_process';
import { cpSync, existsSync, mkdtempSync, rmSync } from 'node:fs';
import { createRequire } from 'node:module';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const example = fileURLToPath(new URL('..', import.meta.url));
const vendor = path.join(example, 'vendor/smugglr-web');
if (!existsSync(vendor)) {
  throw new Error('vendor/smugglr-web is missing: run `pnpm vendor` first');
}
// @smugglr/web's own Playwright, so the example adds no browser dependency.
const { chromium } = createRequire(path.join(example, '../../packages/web/package.json'))('playwright');

const KEY = 'a1'.repeat(32);
const SCHEMA = 'CREATE TABLE notes (id TEXT PRIMARY KEY, owner TEXT NOT NULL, body TEXT);' +
  'CREATE TABLE tags (owner TEXT NOT NULL, name TEXT NOT NULL, n INTEGER, PRIMARY KEY (owner, name));';
const ROWS = [
  "INSERT INTO sessions VALUES ('t-ada', 'ada'), ('t-grace', 'grace')",
  "INSERT INTO notes VALUES ('n1', 'ada', 'it''s ada''s'), ('n2', 'ada', NULL), ('n3', 'grace', 'grace''s')",
  "INSERT INTO tags VALUES ('ada', 'home', 9223372036854775807), ('ada', 'work', 2), ('grace', 'home', 3)",
].join(';');
const QUERY = {
  notes: 'SELECT quote(id), quote(owner), quote(body) FROM notes',
  tags: 'SELECT quote(owner), quote(name), quote(n) FROM tags',
};

const work = mkdtempSync(path.join(tmpdir(), 'hono-tab-'));
const persist = path.join(work, 'state');
const assets = path.join(work, 'public');
cpSync(path.join(example, 'test/public'), assets, { recursive: true });
cpSync(vendor, path.join(assets, 'smugglr-web'), { recursive: true });

const wrangler = (...args) =>
  execFileSync('pnpm', ['exec', 'wrangler', ...args], { cwd: example, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });

// The rows D1 holds, by the person who owns them, as quote() text.
function d1(table, owner) {
  const sql = `${QUERY[table]} WHERE owner = '${owner}' ORDER BY 1, 2`;
  const out = wrangler('d1', 'execute', 'DB', '--local', '--persist-to', persist, '--json', '--command', sql);
  return JSON.parse(out)[0].results.map((row) => Object.values(row));
}

const port = await new Promise((resolve) => {
  const server = createServer().listen(0, '127.0.0.1', () => {
    const { port } = server.address();
    server.close(() => resolve(port));
  });
});
const origin = `http://127.0.0.1:${port}`;

wrangler('d1', 'execute', 'DB', '--local', '--persist-to', persist, '--file', 'schema.sql');
wrangler('d1', 'execute', 'DB', '--local', '--persist-to', persist, '--command', ROWS);

const dev = spawn(
  'pnpm',
  ['exec', 'wrangler', 'dev', '--ip', '127.0.0.1', '--port', String(port), '--persist-to', persist,
    '--assets', assets, '--var', `SMUGGLR_KEY:${KEY}`, '--show-interactive-dev-session=false'],
  { cwd: example, stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env, WRANGLER_SEND_METRICS: 'false' } },
);
let log = '';
const logged = (chunk) => {
  log += chunk;
  if (process.env.SMUGGLR_DEBUG) process.stderr.write(chunk);
};
dev.stdout.on('data', logged);
dev.stderr.on('data', logged);

const browser = await chromium.launch();
let failed = false;
const check = async (name, body) => {
  try {
    let timer;
    const late = new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`no answer in 120 s\n${log.slice(-4000)}`)), 120_000);
    });
    await Promise.race([body(), late]).finally(() => clearTimeout(timer));
    console.log(`ok   ${name}`);
  } catch (err) {
    failed = true;
    console.log(`FAIL ${name}\n     ${String(err?.message ?? err).split('\n').join('\n     ')}`);
  }
};

try {
  for (let tries = 0; ; tries++) {
    try {
      if ((await fetch(`${origin}/`, { signal: AbortSignal.timeout(2000) })).ok) break;
    } catch {}
    if (tries > 300 || dev.exitCode !== null || log.includes('Build failed')) throw new Error(`wrangler dev did not start:\n${log}`);
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  // A fresh browser context: an origin whose tabs hold nothing yet.
  const page = async () => {
    const context = await browser.newContext();
    const p = await context.newPage();
    if (process.env.SMUGGLR_DEBUG) {
      p.on('console', (message) => process.stderr.write(`tab: ${message.text()}\n`));
    }
    await p.goto(`${origin}/`);
    await p.waitForFunction(() => window.web !== undefined);
    return p;
  };
  const options = (name, extra = {}) => ({
    endpoint: `${origin}/place`,
    headers: { authorization: 'Bearer t-ada' },
    schema: SCHEMA,
    name,
    ...extra,
  });
  const tabRows = (p, table) =>
    p.evaluate((sql) => window.tab.query(sql), `${QUERY[table]} ORDER BY 1, 2`);

  await check('a tab opened against the Worker completes its first sync and holds the person\'s rows, and only theirs', async () => {
    const p = await page();
    const record = await p.evaluate(async (o) => {
      window.tab = await window.web.open(o);
      return window.tab.record;
    }, options('first.db'));
    assert.equal(record?.status, 'done', JSON.stringify(record));
    for (const table of ['notes', 'tags']) {
      const here = await tabRows(p, table);
      assert.deepEqual(here, d1(table, 'ada'), `the tab's ${table}`);
      assert.ok(here.every((row) => row.includes("'ada'")), `another person's row in the tab's ${table}`);
    }
  });

  await check('a tab filled by retrieve() from a blob the Worker made, then synced, holds every row D1 holds for the person', async () => {
    const response = await fetch(`${origin}/stash/tab-b`, { headers: { authorization: 'Bearer t-ada' } });
    assert.equal(response.status, 200, await response.clone().text());
    const blob = await response.text();
    // A row changed after the stash arrives with the sync that follows.
    wrangler('d1', 'execute', 'DB', '--local', '--persist-to', persist, '--command',
      "UPDATE notes SET body = 'changed after the stash' WHERE id = 'n2'");
    const p = await page();
    const records = await p.evaluate(
      async ({ o, blob, multi }) => {
        window.tab = await window.web.open(o);
        const opened = window.tab.record;
        const retrieved = await window.tab.retrieve(blob, multi);
        const synced = await window.tab.sync();
        return { opened, retrieved, synced };
      },
      { o: options('retrieved.db', { maxWire: 1 }), blob, multi: { key: KEY, node: 'tab-b' } },
    );
    assert.equal(records.opened?.status, 'too much', JSON.stringify(records.opened));
    assert.equal(records.retrieved?.status, 'done', JSON.stringify(records.retrieved));
    assert.equal(records.synced?.status, 'done', JSON.stringify(records.synced));
    for (const table of ['notes', 'tags']) {
      assert.deepEqual(await tabRows(p, table), d1(table, 'ada'), `the tab's ${table}`);
    }
  });

  await check('a write to another person\'s row, or giving a row another owner, is refused with 403', async () => {
    const write = (changes) =>
      fetch(`${origin}/place`, {
        method: 'POST',
        headers: { authorization: 'Bearer t-ada', 'content-type': 'application/json' },
        body: JSON.stringify({ op: 'write', table: 'notes', changes }),
      });
    const theirs = await write([{ op: 'up', key: ["'n3'"], from: null, to: null, values: ["'n3'", "'ada'", "'mine now'"] }]);
    assert.equal(theirs.status, 403, await theirs.text());
    const given = await write([{ op: 'up', key: ["'n9'"], from: null, to: null, values: ["'n9'", "'grace'", "'for grace'"] }]);
    assert.equal(given.status, 403, await given.text());
    assert.deepEqual(d1('notes', 'grace'), [["'n3'", "'grace'", "'grace''s'"]]);
  });

  await check('a body that is not JSON is refused with 400', async () => {
    const response = await fetch(`${origin}/place`, {
      method: 'POST',
      headers: { authorization: 'Bearer t-ada', 'content-type': 'application/json' },
      body: '{"op":',
    });
    assert.equal(response.status, 400, await response.clone().text());
    assert.equal(await response.text(), 'the body is not JSON');
  });

  await check('no session, no answer', async () => {
    const response = await fetch(`${origin}/place`, { method: 'POST', body: '{"op":"tables"}' });
    assert.equal(response.status, 401);
  });
} finally {
  await browser.close();
  dev.kill();
  rmSync(work, { recursive: true, force: true });
}
process.exit(failed ? 1 : 0);

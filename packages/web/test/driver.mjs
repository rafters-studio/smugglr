// Drives tabs of one origin in headless Chromium for smugglr-web's
// scenarios (FR-SMUGGLR-062).
//
// Usage: node test/driver.mjs <origin> [<profile>]. With a profile folder,
// the tabs run in a browser profile kept on disk there, as a person's are.
// Reads one JSON command per line on
// stdin and answers each with one JSON line on stdout, carrying its id;
// commands run at once, so two tabs can act at the same moment:
//   {"id", "op": "eval", "tab": n, "js": "<expression>"}  evaluates in tab n
//     (opened at <origin>/ on first use) and answers its value;
//   {"id", "op": "close", "tab": n}                        closes tab n;
//   {"id", "op": "kill", "tab": n, "after": ms, "how"}     waits, then closes
//     tab n ("close") or kills its renderer process ("crash"), and answers
//     whether its window.pending was still true when it went;
//   {"id", "op": "clear"}                                  clears the
//     origin's data: every file in its origin private file system.
//
// Needs Playwright's Chromium (`pnpm exec playwright install
// chromium-headless-shell`).

import { createInterface } from 'node:readline';
import { chromium } from 'playwright';

const [origin, profile] = process.argv.slice(2);
const context = profile
  ? await chromium.launchPersistentContext(profile)
  : await (await chromium.launch()).newContext();
const tabs = new Map();
context.on('close', () => process.stderr.write('driver: the browser closed\n'));

async function tab(n) {
  if (!tabs.has(n)) {
    tabs.set(
      n,
      (async () => {
        const page = await context.newPage();
        if (process.env.SMUGGLR_DEBUG) {
          page.on('console', (message) => process.stderr.write(`tab ${n}: ${message.text()}\n`));
        }
        await page.goto(`${origin}/`);
        return page;
      })(),
    );
  }
  return tabs.get(n);
}

const ops = {
  async eval({ tab: n, js }) {
    const page = await tab(n);
    return page.evaluate(js);
  },
  async close({ tab: n }) {
    const page = await tab(n);
    tabs.delete(n);
    await page.close();
    return null;
  },
  async kill({ tab: n, after, how }) {
    const page = await tab(n);
    await new Promise((done) => setTimeout(done, after));
    tabs.delete(n);
    const pending = await page.evaluate(() => window.pending === true).catch(() => false);
    if (how === 'crash') {
      const cdp = await context.newCDPSession(page);
      // Page.crash never answers: the page's crash event says it went.
      const crashed = new Promise((done) => page.once('crash', done));
      cdp.send('Page.crash').catch(() => {});
      await crashed;
    }
    await page.close();
    return pending;
  },
  async clear() {
    const page = await context.newPage();
    await page.goto(`${origin}/`);
    await page.evaluate(async () => {
      const root = await navigator.storage.getDirectory();
      for await (const name of root.keys()) {
        await root.removeEntry(name, { recursive: true });
      }
    });
    await page.close();
    return null;
  },
};

const answer = (line) => process.stdout.write(`${JSON.stringify(line)}\n`);

const lines = createInterface({ input: process.stdin });
lines.on('line', async (line) => {
  const command = JSON.parse(line);
  try {
    answer({ id: command.id, ok: true, value: (await ops[command.op](command)) ?? null });
  } catch (err) {
    answer({ id: command.id, ok: false, error: String(err?.message ?? err) });
  }
});
lines.on('close', async () => {
  await context.close();
});

// Drives tabs of one origin in headless Chromium for smugglr-web's
// scenarios (FR-SMUGGLR-062).
//
// Usage: node test/driver.mjs <origin>. Reads one JSON command per line on
// stdin and answers each with one JSON line on stdout, carrying its id;
// commands run at once, so two tabs can act at the same moment:
//   {"id", "op": "eval", "tab": n, "js": "<expression>"}  evaluates in tab n
//     (opened at <origin>/ on first use) and answers its value;
//   {"id", "op": "close", "tab": n}                        closes tab n;
//   {"id", "op": "clear"}                                  clears the
//     origin's data: every file in its origin private file system.
//
// Needs Playwright's Chromium (`pnpm exec playwright install
// chromium-headless-shell`).

import { createInterface } from 'node:readline';
import { chromium } from 'playwright';

const origin = process.argv[2];
const browser = await chromium.launch();
const context = await browser.newContext();
const tabs = new Map();

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
  await browser.close();
});

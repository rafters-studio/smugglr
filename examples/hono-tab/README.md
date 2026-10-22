# hono-tab

The endpoint a tab keeps in step with. `@smugglr/web` opens a tab's own
database and syncs it with an endpoint the app names; smugglr ships no
server. This Worker (Hono on Cloudflare Workers, over D1) is one way to write
that endpoint, for the signed-in person's rows only.

## What it reaches

D1, through its binding (`DB` in `wrangler.toml`), and nothing else. It holds
no Cloudflare API token. Two things in it are the app's, not Cloudflare
access:

- the session check: a stub here, a bearer token looked up in the
  `sessions` table of the same D1. The app's own goes in its place.
- the signing key for stash blobs, `SMUGGLR_KEY`, 64 hex digits, a Worker
  secret. The app chooses it, and its tabs retrieve with it.

## Whose rows

Every table a tab keeps in step has an `owner` column, and the tab's own
tables are the same (`schema.sql`). The Worker names only those tables, and
reads only the rows whose owner is the signed-in person. A write that touches
another person's row, or gives a row another owner, is refused with 403.

## The place operations

The tab names `POST /place`:

```js
import { open } from '@smugglr/web';

const tab = await open({
  endpoint: 'https://app.example/place',
  headers: { authorization: `Bearer ${session}` },
  schema: SCHEMA,
});
```

Each request is one place operation as JSON: `tables`, `hashes`, `rows`,
`weights` or `write`. Rows travel as SQLite's `quote()` text, each row's hash
is the engine's own (`rowHash`, from `@smugglr/web/engine`), and a write applies
each change only where its row is still at the change's `from`, all in one
D1 batch.

## Filling a tab by retrieve

`GET /stash/:device` stashes the person's rows for that device: the Worker
reads them from D1 and the engine's own stash (`stashRows`, from
`@smugglr/web/engine`) makes the blob, signed with `SMUGGLR_KEY`, node `server`.

```js
const blob = await (await fetch('/stash/laptop', { headers })).text();
await tab.retrieve(blob, { key: SMUGGLR_KEY, node: 'laptop' });
await tab.sync();
```

A first sync too heavy for the wire says to fill the tab this way; the sync
after the retrieve carries only what changed since the blob.

## Run it locally

```console
$ pnpm vendor
$ pnpm install
$ pnpm test
```

`pnpm vendor` packs `@smugglr/web` from this repository into
`vendor/smugglr-web`, which the example installs as `@smugglr/web`. The
Worker imports the engine as an app would, from `@smugglr/web/engine`; on
Workers that export comes ready, its WebAssembly compiled at deploy. `pnpm test`
runs `wrangler dev` with a local D1 and keeps tabs in headless Chromium in
step with it; it needs Playwright's Chromium, as `@smugglr/web`'s own tests
do.

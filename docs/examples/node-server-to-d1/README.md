# node-server-to-d1

A Node script reads a local SQLite database and pushes its contents to a Cloudflare D1 database using the `smugglr` npm package. No CLI involved -- everything runs in-process via WASM.

## Prerequisites

- Node 20+
- pnpm (or npm/yarn)
- A Cloudflare D1 database + API token (see [cli-d1-sync](../cli-d1-sync/) for setup)
- A local SQLite file to sync (the `better-sqlite3` package is used to read it)

## Setup

```sh
pnpm install
cp .env.example .env
# fill in CF_ACCOUNT_ID, CF_API_TOKEN, CF_D1_DATABASE_ID, LOCAL_DB
```

## Run

```sh
node push.mjs
```

## Expected output

```
loaded smugglr (wasm: 110 KB)
push complete: { tables: [ { name: 'users', rowsPushed: 12 }, { name: 'posts', rowsPushed: 3 } ] }
```

## What this demonstrates

- The `smugglr` npm package runs in Node, not just the browser. The WASM binary is identical.
- A `LocalEndpointConfig` with a custom `SqlExecutor` lets you plug in any SQLite runtime -- here `better-sqlite3`, but the same shape works for sql.js, the official sqlite-wasm package, or your own.
- One-shot push with a typed result. Pair with [node-auto-sync](../node-auto-sync/) for a long-running daemon.

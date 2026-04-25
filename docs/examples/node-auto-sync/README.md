# node-auto-sync

A long-running Node process that calls `smugglr.sync()` on an interval, with exponential backoff on transient failures and clean shutdown on `SIGTERM`. This is the manual version of what the upcoming `autoSync` config option will provide; once #113 lands, this becomes a one-liner.

## Prerequisites

Same as [node-server-to-d1](../node-server-to-d1/) -- a local SQLite file and a D1 (or any HTTP-SQL) destination.

## Setup

```sh
pnpm install
cp .env.example .env
# fill in CF_ACCOUNT_ID, CF_API_TOKEN, CF_D1_DATABASE_ID, LOCAL_DB
```

## Run

```sh
node --env-file=.env auto-sync.mjs
```

Send `SIGTERM` (Ctrl-C) to stop. The current sync completes before exit; no half-written batches.

## Expected output

```
[12:30:00.123] sync ok: 3 rows
[12:30:30.456] sync ok: 0 rows
[12:31:00.789] sync failed (retryable): 503 -- backing off 2000ms
[12:31:02.812] sync ok: 7 rows
```

## What this demonstrates

- The `SmugglrError.retryable` flag (true for HTTP 429/503 and timeouts) drives the backoff decision; non-retryable errors propagate.
- The `sync()` method serializes -- you cannot have two in flight at once. The interval pattern below uses a guard to skip ticks while one is still running.
- Bounded backoff (caps at 5 minutes) prevents thundering herd if the remote stays down.

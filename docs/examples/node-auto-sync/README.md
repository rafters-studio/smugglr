# node-auto-sync

A long-running Node process that calls `sync()` on an interval, backs off exponentially on a retryable failure, and finishes the sync in flight before it exits on `SIGTERM`. The package's own `autoSync` option shipped in 0.4.0, but it is browser-only: it hangs off `navigator.locks` and the `online` event and is a no-op in Node. This loop is the Node equivalent.

The output below was captured against the local HTTP-SQL endpoint from [node-server-to-d1](../node-server-to-d1/), on the shared [westwind](../westwind/) sample, with a 503 injected on purpose. The D1 variant is the same script with three lines of `.env` changed; it needs a D1 token and was not run here.

## Prerequisites

Same as [node-server-to-d1](../node-server-to-d1/): Node 24, pnpm, the `sqlite3` shell, and the databases that example builds. Run that example first; this one starts from its `app.sqlite` and `remote.sqlite`, which its push left in sync.

## Setup

```sh
pnpm install
pnpm approve-builds better-sqlite3
cp .env.example .env
cp ../node-server-to-d1/app.sqlite ../node-server-to-d1/remote.sqlite .
```

Start the endpoint in a second terminal, from this directory so it opens the copied `remote.sqlite`.

```sh
node ../node-server-to-d1/local-endpoint.mjs
```

## Run

The capture below sets the interval to two seconds so the run fits on a screen; the `.env` default is 30 seconds. A variable already in the environment wins over `--env-file`.

```sh
SYNC_INTERVAL_MS=2000 node --env-file=.env auto-sync.mjs
```

While it ran, a third terminal made the endpoint fail one query, then added a customer locally, then sent the signal.

```sh
curl -X POST http://127.0.0.1:8765/fail/1
sqlite3 app.sqlite "INSERT INTO customers (id, code, company, contact, title, city, region, updated_at) VALUES ('019910a0-0000-7000-8000-00000000c001', 'ONION', 'Onion Knight Provisioners', 'Davos Seaworth', 'Hand', 'Cape Wrath', 'The Stormlands', 1756411200)"
kill -TERM <pid>
```

```
[22:27:50.595] sync ok: 0 rows
[22:27:52.599] sync failed (retryable): Remote API error: HTTP 503 from http://127.0.0.1:8765: injected outage -- backing off 1000ms
[22:27:53.619] sync ok: 1 rows
[22:27:55.637] sync ok: 0 rows
[22:27:56.258] received SIGTERM, stopping after the current sync
```

The first tick moves nothing because the databases start in sync. The second hits the injected 503 and waits one second instead of two; the third, one second later, pushes the new customer and resets the backoff, so the fourth is back on the two-second interval. Ctrl-C sends `SIGINT`, which the script handles the same way.

## What this demonstrates

`SmugglrError.retryable` drives the backoff decision. The package sets it from the error message: a message that mentions a timeout, HTTP 429, HTTP 503, or a rate limit gets exit code 3 and `retryable: true`; everything else is `false`. A refused connection reports `fetch failed`, which is not retryable, so this loop exits on an endpoint that is down rather than one that is overloaded. Widen the test in `tick()` if your deployment should ride that out.

Two `sync()` calls must never be in flight at once: the WASM instance borrows its endpoints across awaits and does not tolerate a concurrent call. The loop enforces that by construction. A tick schedules the next one with `setTimeout` only after its own `sync()` settles, so there is no interval timer that could fire early and no guard flag to forget.

Backoff doubles from one second and caps at five minutes, so a remote that stays down is polled every five minutes rather than hammered. A success resets it.

`SIGTERM` and `SIGINT` cancel the pending timer, await the `sync()` promise if one is running, and only then dispose the WASM instance and close the database. A batch that is mid-write finishes; a batch that has not started never does.

`newer_wins` orders on Westwind's `updated_at`, which is unix seconds. The inserted customer carries `1756411200`, later than every seeded row, so it would win a conflict on either side.

## Files

| File | Purpose |
| ---- | ------- |
| `auto-sync.mjs` | The loop: WASM load, `better-sqlite3` executor, tick, backoff, shutdown. |
| `.env.example` | Local endpoint values, with the D1 variant commented out. |

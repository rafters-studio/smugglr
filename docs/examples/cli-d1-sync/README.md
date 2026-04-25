# cli-d1-sync

Sync a local SQLite database with a Cloudflare D1 database from the command line.

## Prerequisites

- `smugglr` installed (`brew install rafters-studio/tap/smugglr` or download from [releases](https://github.com/rafters-studio/smugglr/releases))
- A Cloudflare account with D1 enabled
- A D1 database -- create one with `npx wrangler d1 create my-app`
- A Cloudflare API token with `D1:Edit` scope -- create at <https://dash.cloudflare.com/profile/api-tokens>
- A local SQLite file to sync (any `.db`/`.sqlite` file with at least one table)

## Setup

1. Copy `config.example.toml` to `config.toml` and fill in the placeholders:

   ```sh
   cp config.example.toml config.toml
   ```

2. Set your API token in the environment so it doesn't sit in the config file:

   ```sh
   export SMUGGLR_D1_TOKEN="your-cloudflare-api-token"
   ```

3. Make sure the schema exists on both sides. smugglr does not propagate `CREATE TABLE`; create the same tables locally and on D1 (e.g. via `wrangler d1 execute my-app --file=schema.sql`).

## Run

Inspect what would change without writing anything:

```sh
smugglr diff
```

Push local rows to D1:

```sh
smugglr push
```

Pull D1 rows down:

```sh
smugglr pull
```

Bidirectional sync (push then pull, with conflict resolution):

```sh
smugglr sync
```

## Expected output

```
$ smugglr push
[INFO] users: pushing 12 rows
[INFO] posts: pushing 3 rows
push complete: 15 rows in 240ms
```

## What this demonstrates

- The minimum viable smugglr setup: a `config.toml` with a `local` source and a `d1` dest.
- Content-hashed delta: re-running `push` after no local edits reports `0 rows`.
- Agent-friendly mode: add `--output json` to any command to get structured exit codes for scripting.

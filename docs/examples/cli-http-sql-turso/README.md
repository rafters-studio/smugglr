# cli-http-sql-turso

The shape every hosted backend uses. Turso here; rqlite, Datasette, StarbaseDB, SQLite Cloud, and a generic `{sql, params}` endpoint are the same config with a different `profile` and `url`.

## Prerequisites

`smugglr` and `smugglr-http-sql` on your PATH (`cargo install smugglr` and `cargo install smugglr-http-sql`, or the release archive from 0.5.1, which carries both), a Turso database and a token (`turso db create my-app`, `turso db tokens create my-app`), and the [westwind](../westwind/) sample or any local SQLite whose tables also exist on the remote.

## Setup

```sh
cp config.example.toml config.toml
export TURSO_TOKEN="your-turso-token"
../westwind/make.sh ./local.db
```

`config.example.toml`:

```toml
local_db = "./local.db"

[target]
type = "plugin"
name = "http-sql"

[target.config]
profile = "turso"
url = "https://my-db.turso.io"
auth_token = "${TURSO_TOKEN}"
```

`name` is the plugin binary, `smugglr-http-sql`, resolved from `~/.smugglr/plugins/` or `$PATH`. `profile` picks the backend's request and response shape: `d1`, `turso`, `rqlite`, `datasette`, `sqlite-cloud`, `starbasedb`, `generic`, `http-sql`. `name = "turso"` is a mistake that fails with "Plugin 'turso' not found".

## Run

```sh
smugglr status
smugglr push --dry-run
smugglr push
```

The summary block is the same one [cli-sqlite-to-sqlite](../cli-sqlite-to-sqlite/) shows, against a remote. No output is shown here because nobody maintaining this example holds a Turso token; the config parses and reaches the plugin, which is as far as this directory was run. Two facts to know before running it for real: the Turso profile posts its request body to `url` exactly as given, and whether a bare host or `/v2/pipeline` is the endpoint that answers is recorded as a test to write under smugglr#436; and in 0.5.0 the plugin reports a 429 or 5xx as permanent, so a rate limit ends the push on the first response (smugglr#432).

## What this demonstrates

One plugin, one config shape, every hosted backend. Changing the vendor is a profile name, not a recompile.

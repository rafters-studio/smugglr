# @smugglr/nanostores

Nanostores adapter that auto-persists atoms and maps to a smugglr-managed SQLite table and rehydrates from sync events. Same wedge as [@smugglr/zustand](../zustand-plugin/) -- keep your atoms, plug in smugglr, get sync.

## Install

```sh
pnpm add @smugglr/nanostores smugglr nanostores
```

## Usage

```ts
import { atom } from "nanostores";
import { Smugglr, createWaSqliteExecutor } from "smugglr";
import { smuggl } from "@smugglr/nanostores";

const executor = createWaSqliteExecutor(sqlite3, db);
await executor.run(
  `CREATE TABLE IF NOT EXISTS app_state (
     key TEXT PRIMARY KEY,
     value TEXT NOT NULL,
     updated_at TEXT
   )`,
  [],
);

const smugglrInstance = await Smugglr.init({
  source: { type: "local", executor },
  dest: { url: "https://my-app.turso.io", authToken: "...", profile: "turso" },
  sync: { tables: ["app_state"], conflictResolution: "newer_wins" },
});

const $todos = atom<string[]>([]);

const dispose = smuggl($todos, {
  smugglr: smugglrInstance,
  executor,
  table: "app_state",
  key: "todos",
});
```

That's it. Three things happen automatically:

- **On mount**, the adapter SELECTs the `todos` row and `atom.set()`s the parsed value.
- **On every change**, it serializes the atom value and upserts the row.
- **On every sync that touches this row**, it re-reads and pushes the new value back into the atom.

The returned `dispose()` detaches both listeners. The persisted row stays put.

## Map stores

`map` stores work the same way. The whole map value is serialized as a single JSON object:

```ts
import { map } from "nanostores";
const $user = map<{ name: string; email: string }>({ name: "", email: "" });
smuggl($user, { smugglr, executor, table: "app_state", key: "user" });
```

`computed` and `deepMap` are out of scope for v0.1; open an issue if you need them.

## Options

| Option        | Required | Description |
| ------------- | -------- | ----------- |
| `smugglr`     | yes      | Smugglr instance for `.on("table-changed", ...)`. |
| `executor`    | yes      | `SqlExecutor` -- the same one wrapping your local SQLite. |
| `table`       | yes      | Persistence table name. Caller owns the DDL. |
| `key`         | yes      | Primary key for this atom's row. |
| `serialize`   | no       | Custom serializer. Defaults to `JSON.stringify`. |
| `deserialize` | no       | Custom deserializer. Defaults to `JSON.parse`. |
| `onHydrate`   | no       | Callback fired once after the initial hydration query completes. |

## Multiple atoms, one table

Use distinct `key` values:

```ts
smuggl($todos, { ..., table: "app_state", key: "todos" });
smuggl($auth,  { ..., table: "app_state", key: "auth" });
smuggl($prefs, { ..., table: "app_state", key: "prefs" });
```

A sync that touches one row only re-hydrates that one atom; smugglr's `table-changed` event includes the affected primary keys.

## License

MIT

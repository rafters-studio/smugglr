# @smugglr/zustand

Zustand middleware that auto-persists store slices to a smugglr-managed SQLite table and rehydrates from sync events. Keep Zustand. Mark the slice. Smugglr handles persistence and sync.

## Install

```sh
pnpm add @smugglr/zustand smugglr zustand
```

## Usage

```ts
import { create } from "zustand";
import { Smugglr, createWaSqliteExecutor } from "smugglr";
import { smuggl } from "@smugglr/zustand";

// 1. Set up your local SQLite + smugglr instance.
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

// 2. Wire the middleware into your store.
interface AppState {
  todos: string[];
  addTodo: (todo: string) => void;
}

const useStore = create<AppState>()(
  smuggl(
    (set) => ({
      todos: [],
      addTodo: (t) => set((s) => ({ todos: [...s.todos, t] })),
    }),
    {
      smugglr: smugglrInstance,
      executor,
      table: "app_state",
      key: "todos",
      include: (s) => ({ todos: s.todos }),
    },
  ),
);
```

That's it. Three things happen automatically:

- **On mount**, the middleware reads the `todos` row from `app_state` and seeds the store.
- **On every `set()`**, it serializes the projection (`include(state)`) and upserts the row.
- **On every `smugglr.sync()` that touches this row**, it reads the new value and merges it back into the store via `set()` -- so other browser tabs, other devices, or a server-side update show up live without a page reload.

## Options

| Option        | Required | Description |
| ------------- | -------- | ----------- |
| `smugglr`     | yes      | Smugglr instance. Used for `.on("table-changed", ...)`. |
| `executor`    | yes      | `SqlExecutor` -- the same one wrapping your local SQLite. Used for direct upserts and reads of the persistence row. |
| `table`       | yes      | Persistence table name. Caller owns the DDL. |
| `key`         | yes      | Primary key for this store's row. Use distinct keys when multiple stores share one table. |
| `include`     | no       | `(state) => projection`. Skip ephemeral fields from the persisted slice. Defaults to identity. |
| `serialize`   | no       | Custom serializer. Defaults to `JSON.stringify`. |
| `deserialize` | no       | Custom deserializer. Defaults to `JSON.parse`. |
| `onHydrate`   | no       | Callback fired once after the initial hydration query completes. |

## Why this exists

RxDB, ElectricSQL, and PowerSync all force you to abandon your state library and adopt theirs. Smugglr's pitch: keep the store you already have. The middleware is ~150 lines because the heavy lifting -- delta sync, conflict resolution, content hashing -- already lives in smugglr.

## Multiple stores, one table

Multiple stores can share the same persistence table by using distinct `key` values:

```ts
const useTodos = create()(smuggl(initTodos, { ..., table: "app_state", key: "todos" }));
const useAuth = create()(smuggl(initAuth, { ..., table: "app_state", key: "auth" }));
```

Each store reads and writes only its own row. Smugglr's `table-changed` events scope by primary key, so a sync that touches `todos` does not re-hydrate `auth`.

## License

MIT

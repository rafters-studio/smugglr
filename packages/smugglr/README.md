# smugglr

Content-hashed delta sync for SQLite, in the browser.

The smugglr sync engine compiled to WebAssembly with a small typed wrapper. Push, pull, or bidirectionally sync between any two HTTP-SQL endpoints (Turso, rqlite, Cloudflare D1, custom) directly from the browser or Node. Only rows that actually changed are sent.

Homepage: [smugglr.dev](https://smugglr.dev)

## Install

```sh
pnpm add smugglr
```

## Usage

```ts
import { Smugglr } from "smugglr";

const s = await Smugglr.init({
  source: { url: "https://my-db.turso.io", authToken: "tok", profile: "turso" },
  dest:   { url: "https://api.cloudflare.com/...", authToken: "cf", profile: "d1" },
  sync:   { tables: ["users", "posts"], conflictResolution: "local_wins" },
});

await s.sync();          // bidirectional
await s.push();          // source -> dest
await s.pull();          // dest -> source
const plan = await s.diff(); // dry-run plan, no writes

s.dispose();
```

`using` is supported:

```ts
using s = await Smugglr.init(config);
await s.sync();
// freed at scope exit
```

## Custom WASM loading

If your bundler resolves `.wasm` imports differently or you serve the binary from a CDN:

```ts
import { Smugglr, setWasm } from "smugglr";
import * as wasm from "smugglr/wasm";

await wasm.default("https://cdn.example.com/smugglr_wasm_bg.wasm");
setWasm(wasm);

const s = await Smugglr.init(config);
```

## License

MIT

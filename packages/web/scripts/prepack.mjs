// Lays out @smugglr/web before it is packed, so an app that installs it
// imports one module and needs no paths, files or build step of its own:
// the engine (smugglr-web, built with wasm-pack) goes to src/engine and
// SQLite's browser build to src/sqlite, beside src/index.js, which loads
// both relative to itself.

import { execFileSync } from 'node:child_process';
import { copyFileSync, mkdirSync, rmSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const web = fileURLToPath(new URL('..', import.meta.url));
const src = path.join(web, 'src');

rmSync(path.join(src, 'engine'), { recursive: true, force: true });
execFileSync(
  'wasm-pack',
  [
    'build',
    path.join(web, '../../crates/smugglr-web'),
    '--target',
    'web',
    '--release',
    '--no-pack',
    '--out-dir',
    path.join(src, 'engine'),
  ],
  { stdio: 'inherit' },
);
rmSync(path.join(src, 'engine', '.gitignore'), { force: true });

const sqlite = path.join(web, 'node_modules/@sqlite.org/sqlite-wasm/dist');
mkdirSync(path.join(src, 'sqlite'), { recursive: true });
for (const file of ['index.mjs', 'sqlite3.wasm', 'sqlite3-opfs-async-proxy.js']) {
  copyFileSync(path.join(sqlite, file), path.join(src, 'sqlite', file));
}

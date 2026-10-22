// Packs @smugglr/web from this repository, as an app would install it, and
// lays it out in vendor/smugglr-web: the example installs it from there as
// @smugglr/web, and the test's tab page loads it.

import { execFileSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const example = fileURLToPath(new URL('..', import.meta.url));
const web = path.join(example, '../../packages/web');
const vendor = path.join(example, 'vendor/smugglr-web');

const packed = mkdtempSync(path.join(tmpdir(), 'smugglr-web-'));
execFileSync('pnpm', ['install', '--frozen-lockfile'], { cwd: web, stdio: 'inherit' });
execFileSync('pnpm', ['pack', '--pack-destination', packed], { cwd: web, stdio: 'inherit' });
const tarball = readdirSync(packed).find((name) => name.endsWith('.tgz'));
if (tarball === undefined) {
  throw new Error(`pnpm pack wrote no tarball into ${packed}`);
}

rmSync(vendor, { recursive: true, force: true });
mkdirSync(vendor, { recursive: true });
execFileSync('tar', ['-xzf', path.join(packed, tarball), '-C', vendor, '--strip-components', '1']);
rmSync(packed, { recursive: true, force: true });

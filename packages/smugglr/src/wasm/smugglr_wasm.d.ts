// Type stub for the wasm-bindgen output.
// The actual JS + WASM files are copied into dist/wasm/ at build time.

export class Smugglr {
  private constructor();
  free(): void;
  static init(config_js: unknown): Smugglr;
  push(dry_run?: boolean | null): Promise<unknown>;
  pull(dry_run?: boolean | null): Promise<unknown>;
  sync(dry_run?: boolean | null): Promise<unknown>;
  diff(): Promise<unknown>;
  [Symbol.dispose](): void;
}

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export default function init(module_or_path?: InitInput | Promise<InitInput>): Promise<unknown>;

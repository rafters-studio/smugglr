// WebAssembly the Worker imports, compiled at deploy (wrangler.toml's rules).
declare module "*.wasm" {
  const module: WebAssembly.Module;
  export default module;
}

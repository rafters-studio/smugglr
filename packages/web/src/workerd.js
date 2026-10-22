// The engine on Cloudflare Workers, as @smugglr/web/engine resolves there
// (the workerd condition): a Worker cannot fetch the engine's WebAssembly, so
// it is imported as a module, compiled at deploy, and instantiated here.

import module from './engine/smugglr_web_bg.wasm';
import { initSync } from './engine/smugglr_web.js';

initSync({ module });

export * from './engine/smugglr_web.js';

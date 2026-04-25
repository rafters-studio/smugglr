import { defineConfig } from "vite";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  root: here,
  server: {
    port: 5173,
    strictPort: true,
    fs: {
      // Allow serving from the package root so dist/ + node_modules/wa-sqlite resolve.
      allow: [resolve(here, "..")],
    },
  },
  optimizeDeps: {
    // wa-sqlite ships ESM with a .wasm sidecar; let Vite handle it but skip
    // the prebundle step (which mangles the Emscripten loader).
    exclude: ["wa-sqlite"],
  },
  build: {
    target: "es2022",
  },
});

import { defineConfig } from "vite";

export default defineConfig({
  optimizeDeps: {
    exclude: ["wa-sqlite"],
  },
  build: {
    target: "es2022",
  },
});

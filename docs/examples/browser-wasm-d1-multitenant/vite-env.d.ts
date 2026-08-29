/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_GUARD_URL?: string;
  // VITE_TENANT_TOKEN_<TENANT> entries are read by name at runtime.
  readonly [key: string]: string | undefined;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}

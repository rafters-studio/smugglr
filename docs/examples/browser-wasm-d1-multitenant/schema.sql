-- D1 schema for the multi-tenant example.
--
-- Every table has a tenant_id column. The fence Worker validates that writes
-- carry the authenticated tenant and scopes reads by it. The index on
-- (tenant_id, id) keeps per-tenant scans cheap as the shared table grows.

CREATE TABLE IF NOT EXISTS notes (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  body TEXT,
  updated_at TEXT
);

CREATE INDEX IF NOT EXISTS notes_tenant_id_idx ON notes (tenant_id, id);

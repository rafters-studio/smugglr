-- Minimal example schema. Apply locally with `sqlite3 app.sqlite < schema.sql`
-- and to D1 with `npx wrangler d1 execute my-app --file=schema.sql`.

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,         -- UUIDv7 recommended
    email TEXT NOT NULL UNIQUE,
    name TEXT,
    updated_at TEXT NOT NULL     -- ISO 8601, e.g. 2026-04-25T03:14:15Z
);

CREATE TABLE IF NOT EXISTS posts (
    id TEXT PRIMARY KEY,
    author_id TEXT NOT NULL REFERENCES users(id),
    title TEXT NOT NULL,
    body TEXT,
    updated_at TEXT NOT NULL
);

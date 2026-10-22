-- The app's tables. Every table a tab keeps in step has an `owner` column,
-- and a tab's own tables are the same. `sessions` is the Worker's alone, so
-- it has no `owner` column and no tab ever sees it.
CREATE TABLE notes (id TEXT PRIMARY KEY, owner TEXT NOT NULL, body TEXT);
CREATE TABLE tags (owner TEXT NOT NULL, name TEXT NOT NULL, n INTEGER, PRIMARY KEY (owner, name));
CREATE TABLE sessions (token TEXT PRIMARY KEY, person TEXT NOT NULL);

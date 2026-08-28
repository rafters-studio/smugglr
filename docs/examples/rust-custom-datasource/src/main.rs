//! Implementing `DataSource` against an in-memory store.
//!
//! Replace `Mutex<HashMap<...>>` with whatever your real store is (Redis,
//! object-store JSON blob, a custom HTTP API). The trait surface is the same.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use anyhow::Result;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use smugglr_core::config::Config;
use smugglr_core::datasource::{ColumnInfo, DataSource, RowMeta, TableInfo};
use smugglr_core::error::Result as SmugglrResult;
use smugglr_core::sync::{sync_all, NoProgress};

/// One row's stored shape: a JSON object plus a derived content hash.
struct Row {
    data: HashMap<String, JsonValue>,
}

impl Row {
    fn content_hash(&self) -> String {
        // Stable hash: serialize keys in sorted order so two equivalent rows
        // always hash the same regardless of HashMap iteration order.
        let mut keys: Vec<&String> = self.data.keys().collect();
        keys.sort();
        let mut hasher = Sha256::new();
        for k in keys {
            hasher.update(k.as_bytes());
            hasher.update(b"=");
            hasher.update(self.data[k].to_string().as_bytes());
            hasher.update(b";");
        }
        hex::encode(hasher.finalize())
    }
}

type Tables = HashMap<String, HashMap<String, Row>>;

struct InMemoryStore {
    name: &'static str,
    tables: Mutex<Tables>,
}

impl InMemoryStore {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            tables: Mutex::new(HashMap::new()),
        }
    }

    fn tables(&self) -> MutexGuard<'_, Tables> {
        self.tables.lock().expect("store mutex poisoned")
    }

    fn upsert(&self, table: &str, pk: &str, row: HashMap<String, JsonValue>) {
        let mut tables = self.tables();
        let t = tables.entry(table.to_string()).or_default();
        t.insert(pk.to_string(), Row { data: row });
    }

    /// Render one table as `pk value @ updated_at` lines, sorted by primary key.
    fn dump(&self, table: &str) {
        let tables = self.tables();
        let mut pks: Vec<&String> = tables
            .get(table)
            .map(|t| t.keys().collect())
            .unwrap_or_default();
        pks.sort();
        for pk in pks {
            let row = &tables[table][pk];
            let field = |k: &str| row.data.get(k).and_then(|v| v.as_str()).unwrap_or("NULL");
            println!(
                "  {}: {} {} @ {}",
                self.name,
                pk,
                field("value"),
                field("updated_at")
            );
        }
    }
}

impl DataSource for InMemoryStore {
    async fn list_tables(&self) -> SmugglrResult<Vec<String>> {
        Ok(self.tables().keys().cloned().collect())
    }

    async fn table_info(&self, table: &str) -> SmugglrResult<TableInfo> {
        // Schema is implicit in our store; we just declare {id, value, updated_at}.
        let column = |name: &str, pk: bool| ColumnInfo {
            name: name.into(),
            col_type: "TEXT".into(),
            notnull: pk,
            pk,
        };
        Ok(TableInfo {
            name: table.to_string(),
            columns: vec![
                column("id", true),
                column("value", false),
                column("updated_at", false),
            ],
            primary_key: vec!["id".into()],
        })
    }

    async fn get_row_metadata(
        &self,
        table: &str,
        timestamp_column: &str,
        _exclude_columns: &[String],
    ) -> SmugglrResult<HashMap<String, RowMeta>> {
        let tables = self.tables();
        let Some(t) = tables.get(table) else {
            return Ok(HashMap::new());
        };
        Ok(t.iter()
            .map(|(pk, row)| {
                let updated_at = row
                    .data
                    .get(timestamp_column)
                    .and_then(|v| v.as_str())
                    .map(String::from);
                (
                    pk.clone(),
                    RowMeta {
                        pk_value: pk.clone(),
                        updated_at,
                        content_hash: row.content_hash(),
                    },
                )
            })
            .collect())
    }

    async fn get_rows(
        &self,
        table: &str,
        pk_values: &[String],
    ) -> SmugglrResult<Vec<HashMap<String, JsonValue>>> {
        let tables = self.tables();
        let Some(t) = tables.get(table) else {
            return Ok(Vec::new());
        };
        Ok(pk_values
            .iter()
            .filter_map(|pk| t.get(pk).map(|r| r.data.clone()))
            .collect())
    }

    async fn upsert_rows(
        &self,
        table: &str,
        rows: &[HashMap<String, JsonValue>],
    ) -> SmugglrResult<usize> {
        // An incoming row may omit columns (see the trait doc on `upsert_rows`):
        // a column the row does not mention keeps whatever the store already
        // holds for it. Merging into the existing row honors that.
        let mut tables = self.tables();
        let t = tables.entry(table.to_string()).or_default();
        let mut written = 0;
        for row in rows {
            let Some(pk) = row.get("id").and_then(|v| v.as_str()) else {
                continue;
            };
            let existing = t.entry(pk.to_string()).or_insert_with(|| Row {
                data: HashMap::new(),
            });
            existing.data.extend(row.clone());
            written += 1;
        }
        Ok(written)
    }

    async fn row_count(&self, table: &str) -> SmugglrResult<usize> {
        Ok(self.tables().get(table).map_or(0, |t| t.len()))
    }
}

fn widget(id: &str, value: &str, updated_at: &str) -> HashMap<String, JsonValue> {
    HashMap::from([
        ("id".into(), JsonValue::String(id.into())),
        ("value".into(), JsonValue::String(value.into())),
        ("updated_at".into(), JsonValue::String(updated_at.into())),
    ])
}

#[tokio::main]
async fn main() -> Result<()> {
    let a = InMemoryStore::new("a");
    let b = InMemoryStore::new("b");

    // Seed both sides with overlapping but divergent data: w1 only on a, w2
    // only on b, and w3 on both with b holding the newer edit.
    a.upsert(
        "widgets",
        "w1",
        widget("w1", "alpha", "2026-04-25T00:00:00Z"),
    );
    a.upsert(
        "widgets",
        "w3",
        widget("w3", "gamma", "2026-04-25T00:00:00Z"),
    );
    b.upsert(
        "widgets",
        "w2",
        widget("w2", "beta", "2026-04-25T00:00:01Z"),
    );
    b.upsert(
        "widgets",
        "w3",
        widget("w3", "gamma-edited", "2026-04-25T00:00:05Z"),
    );

    println!(
        "before: a={}, b={}",
        a.row_count("widgets").await?,
        b.row_count("widgets").await?
    );
    a.dump("widgets");
    b.dump("widgets");

    // `newer_wins` compares `updated_at` when the content hashes differ. The
    // default, `local_wins`, would keep a's copy of w3.
    let config = Config::from_toml_str("[sync]\nconflict_resolution = \"newer_wins\"\n")?;
    let results = sync_all(
        &a,
        &b,
        &config,
        Some(vec!["widgets".into()]),
        false,
        &NoProgress,
    )
    .await?;
    for r in &results {
        println!(
            "sync:   {} pushed a->b={}, pulled b->a={}",
            r.table, r.rows_pushed, r.rows_pulled
        );
    }

    println!(
        "after:  a={}, b={}",
        a.row_count("widgets").await?,
        b.row_count("widgets").await?
    );
    a.dump("widgets");
    b.dump("widgets");
    Ok(())
}

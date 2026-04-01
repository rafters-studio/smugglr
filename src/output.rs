//! Structured output for agent-friendly JSON responses.
//!
//! When `--output json` is passed, commands emit a single JSON object to stdout
//! instead of human-readable text. The watch daemon emits one JSON line per tick
//! (JSONL format).

use crate::diff::TableDiff;
use crate::sync::SyncResult;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Text,
    Json,
}

impl std::str::FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            _ => Err(format!(
                "invalid output format '{}', expected 'text' or 'json'",
                s
            )),
        }
    }
}

#[derive(Serialize)]
pub struct CommandOutput {
    pub command: &'static str,
    pub status: &'static str,
    pub dry_run: bool,
    pub tables: Vec<TableOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct TableOutput {
    pub name: String,
    #[serde(skip_serializing_if = "is_zero")]
    pub rows_pushed: usize,
    #[serde(skip_serializing_if = "is_zero")]
    pub rows_pulled: usize,
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

#[derive(Serialize)]
pub struct DiffOutput {
    pub command: &'static str,
    pub status: &'static str,
    pub tables: Vec<TableDiffOutput>,
}

#[derive(Serialize)]
pub struct TableDiffOutput {
    pub name: String,
    pub local_only: Vec<String>,
    pub remote_only: Vec<String>,
    pub local_newer: Vec<String>,
    pub remote_newer: Vec<String>,
    pub content_differs: Vec<String>,
    pub identical_count: usize,
}

#[derive(Serialize)]
pub struct StatusOutput {
    pub command: &'static str,
    pub status: &'static str,
    pub config: StatusConfig,
    pub local: StatusDb,
    pub target: StatusDb,
}

#[derive(Serialize)]
pub struct StatusConfig {
    pub local_db: String,
    pub target_type: String,
    pub timestamp_column: String,
    pub conflict_resolution: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub exclude_tables: Vec<String>,
}

#[derive(Serialize)]
pub struct StatusDb {
    pub connected: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub tables: Vec<StatusTable>,
}

#[derive(Serialize)]
pub struct StatusTable {
    pub name: String,
    pub rows: usize,
}

#[derive(Serialize)]
pub struct WatchTickOutput {
    pub command: &'static str,
    pub tick: u64,
    pub status: &'static str,
    pub tables: Vec<TableOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct ErrorOutput {
    pub command: &'static str,
    pub status: &'static str,
    pub error: String,
    pub exit_code: i32,
}

impl CommandOutput {
    pub fn from_sync_results(command: &'static str, results: &[SyncResult], dry_run: bool) -> Self {
        Self {
            command,
            status: "ok",
            dry_run,
            tables: results
                .iter()
                .filter(|r| r.has_changes())
                .map(|r| TableOutput {
                    name: r.table.clone(),
                    rows_pushed: r.rows_pushed,
                    rows_pulled: r.rows_pulled,
                })
                .collect(),
            error: None,
        }
    }
}

impl DiffOutput {
    pub fn from_diffs(diffs: Vec<(String, TableDiff)>) -> Self {
        Self {
            command: "diff",
            status: "ok",
            tables: diffs
                .into_iter()
                .map(|(name, d)| TableDiffOutput {
                    name,
                    identical_count: d.identical.len(),
                    local_only: d.local_only,
                    remote_only: d.remote_only,
                    local_newer: d.local_newer,
                    remote_newer: d.remote_newer,
                    content_differs: d.content_differs,
                })
                .collect(),
        }
    }
}

impl WatchTickOutput {
    pub fn from_results(tick: u64, results: &[SyncResult]) -> Self {
        Self {
            command: "watch",
            tick,
            status: "ok",
            tables: results
                .iter()
                .filter(|r| r.has_changes())
                .map(|r| TableOutput {
                    name: r.table.clone(),
                    rows_pushed: r.rows_pushed,
                    rows_pulled: r.rows_pulled,
                })
                .collect(),
            error: None,
        }
    }

    pub fn from_error(tick: u64, err: &str) -> Self {
        Self {
            command: "watch",
            tick,
            status: "error",
            tables: vec![],
            error: Some(err.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::TableDiff;
    use crate::sync::SyncResult;

    #[test]
    fn test_output_format_parse() {
        assert_eq!("text".parse::<OutputFormat>().unwrap(), OutputFormat::Text);
        assert_eq!("json".parse::<OutputFormat>().unwrap(), OutputFormat::Json);
        assert!("xml".parse::<OutputFormat>().is_err());
    }

    #[test]
    fn test_command_output_json_serialization() {
        let results = vec![
            SyncResult {
                table: "abilities".into(),
                rows_pushed: 42,
                rows_pulled: 0,
            },
            SyncResult {
                table: "items".into(),
                rows_pushed: 0,
                rows_pulled: 0,
            },
        ];

        let out = CommandOutput::from_sync_results("push", &results, false);
        let json = serde_json::to_string(&out).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(v["command"], "push");
        assert_eq!(v["status"], "ok");
        assert_eq!(v["dry_run"], false);
        // Only abilities should appear (items had 0 changes)
        assert_eq!(v["tables"].as_array().unwrap().len(), 1);
        assert_eq!(v["tables"][0]["name"], "abilities");
        assert_eq!(v["tables"][0]["rows_pushed"], 42);
        // error should be absent (skip_serializing_if)
        assert!(v.get("error").is_none());
    }

    #[test]
    fn test_command_output_dry_run() {
        let results = vec![SyncResult {
            table: "t".into(),
            rows_pushed: 10,
            rows_pulled: 5,
        }];

        let out = CommandOutput::from_sync_results("sync", &results, true);
        let json = serde_json::to_string(&out).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(v["dry_run"], true);
        assert_eq!(v["tables"][0]["rows_pushed"], 10);
        assert_eq!(v["tables"][0]["rows_pulled"], 5);
    }

    #[test]
    fn test_diff_output_json_serialization() {
        let mut diff = TableDiff::new("abilities");
        diff.local_only = vec!["pk1".into(), "pk2".into()];
        diff.remote_only = vec!["pk3".into()];
        diff.identical = vec!["pk4".into(), "pk5".into()];

        let out = DiffOutput::from_diffs(vec![("abilities".into(), diff)]);
        let json = serde_json::to_string(&out).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(v["command"], "diff");
        assert_eq!(v["tables"][0]["name"], "abilities");
        assert_eq!(v["tables"][0]["local_only"].as_array().unwrap().len(), 2);
        assert_eq!(v["tables"][0]["remote_only"].as_array().unwrap().len(), 1);
        assert_eq!(v["tables"][0]["identical_count"], 2);
    }

    #[test]
    fn test_watch_tick_output() {
        let results = vec![SyncResult {
            table: "t".into(),
            rows_pushed: 3,
            rows_pulled: 7,
        }];

        let out = WatchTickOutput::from_results(5, &results);
        let json = serde_json::to_string(&out).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(v["command"], "watch");
        assert_eq!(v["tick"], 5);
        assert_eq!(v["status"], "ok");
        assert!(v.get("error").is_none());
    }

    #[test]
    fn test_watch_tick_error_output() {
        let out = WatchTickOutput::from_error(3, "connection timeout");
        let json = serde_json::to_string(&out).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(v["status"], "error");
        assert_eq!(v["tick"], 3);
        assert_eq!(v["error"], "connection timeout");
    }

    #[test]
    fn test_error_output_json() {
        let out = ErrorOutput {
            command: "push",
            status: "error",
            error: "Config file not found".into(),
            exit_code: 2,
        };
        let json = serde_json::to_string(&out).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(v["command"], "push");
        assert_eq!(v["status"], "error");
        assert_eq!(v["exit_code"], 2);
    }

    #[test]
    fn test_status_output_json() {
        let out = StatusOutput {
            command: "status",
            status: "ok",
            config: StatusConfig {
                local_db: "game.db".into(),
                target_type: "sqlite".into(),
                timestamp_column: "updated_at".into(),
                conflict_resolution: "NewerWins".into(),
                tables: vec![],
                exclude_tables: vec![],
            },
            local: StatusDb {
                connected: true,
                error: None,
                tables: vec![StatusTable {
                    name: "abilities".into(),
                    rows: 100,
                }],
            },
            target: StatusDb {
                connected: true,
                error: None,
                tables: vec![StatusTable {
                    name: "abilities".into(),
                    rows: 95,
                }],
            },
        };
        let json = serde_json::to_string(&out).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(v["command"], "status");
        assert_eq!(v["config"]["local_db"], "game.db");
        assert_eq!(v["local"]["connected"], true);
        assert_eq!(v["local"]["tables"][0]["rows"], 100);
        assert_eq!(v["target"]["tables"][0]["rows"], 95);
    }
}

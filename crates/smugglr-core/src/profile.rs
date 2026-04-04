//! Target profiles for different HTTP SQL endpoints.
//!
//! Each profile describes how to format requests and parse responses
//! for a specific HTTP SQL platform. This module lives in smugglr-core
//! so both the http-sql plugin (reqwest) and the WASM adapter (fetch)
//! can share profile definitions without duplication.

use serde_json::Value;

/// How to talk to a specific HTTP SQL endpoint.
#[derive(Debug, Clone)]
pub struct Profile {
    /// How to format the Authorization header
    pub auth_format: AuthFormat,
    /// How to build the request body from a SQL statement + params
    pub request_format: RequestFormat,
    /// JSON path to extract rows from the response
    pub rows_path: Vec<String>,
    /// JSON path to extract column names from the response
    pub columns_path: Vec<String>,
    /// Maximum bind parameters per query (0 = no limit)
    pub max_bind_params: usize,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum AuthFormat {
    Bearer,
    Basic,
    None,
}

#[derive(Debug, Clone)]
pub enum RequestFormat {
    /// Turso/libSQL pipeline: `{"requests": [{"type": "execute", "stmt": {"sql": "<sql>", "args": [...]}}]}`
    Turso,
    /// rqlite: `[["<sql>", ...params]]`
    Rqlite,
    /// Cloudflare D1 REST: `{"sql": "<sql>", "params": [...]}`
    D1,
    /// Datasette: `{"sql": "<sql>", "params": {...}}`
    Datasette,
    /// Flat JSON: `{"sql": "<sql>", "params": [...]}`
    Generic,
}

impl Profile {
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "turso" | "libsql" => Some(Self::turso()),
            "rqlite" => Some(Self::rqlite()),
            "d1" | "cloudflare-d1" => Some(Self::d1()),
            "datasette" => Some(Self::datasette()),
            "sqlite-cloud" | "sqlitecloud" => Some(Self::sqlite_cloud()),
            "starbasedb" | "starbase" => Some(Self::starbasedb()),
            "generic" => Some(Self::generic()),
            _ => None,
        }
    }

    pub fn turso() -> Self {
        Self {
            auth_format: AuthFormat::Bearer,
            request_format: RequestFormat::Turso,
            rows_path: vec![
                "results".into(),
                "0".into(),
                "response".into(),
                "result".into(),
                "rows".into(),
            ],
            columns_path: vec![
                "results".into(),
                "0".into(),
                "response".into(),
                "result".into(),
                "cols".into(),
            ],
            max_bind_params: 0,
        }
    }

    pub fn rqlite() -> Self {
        Self {
            auth_format: AuthFormat::Basic,
            request_format: RequestFormat::Rqlite,
            rows_path: vec!["results".into(), "0".into(), "values".into()],
            columns_path: vec!["results".into(), "0".into(), "columns".into()],
            max_bind_params: 0,
        }
    }

    pub fn d1() -> Self {
        Self {
            auth_format: AuthFormat::Bearer,
            request_format: RequestFormat::D1,
            rows_path: vec!["result".into(), "0".into(), "results".into()],
            columns_path: vec!["result".into(), "0".into(), "results".into()],
            max_bind_params: 100,
        }
    }

    pub fn datasette() -> Self {
        Self {
            auth_format: AuthFormat::Bearer,
            request_format: RequestFormat::Datasette,
            rows_path: vec!["rows".into()],
            columns_path: vec!["columns".into()],
            max_bind_params: 0,
        }
    }

    pub fn sqlite_cloud() -> Self {
        Self {
            auth_format: AuthFormat::Bearer,
            request_format: RequestFormat::Generic,
            rows_path: vec!["data".into()],
            columns_path: vec!["columns".into()],
            max_bind_params: 0,
        }
    }

    pub fn starbasedb() -> Self {
        Self {
            auth_format: AuthFormat::Bearer,
            request_format: RequestFormat::Generic,
            rows_path: vec!["result".into()],
            columns_path: vec!["columns".into()],
            max_bind_params: 0,
        }
    }

    pub fn generic() -> Self {
        Self {
            auth_format: AuthFormat::Bearer,
            request_format: RequestFormat::Generic,
            rows_path: vec!["rows".into()],
            columns_path: vec!["columns".into()],
            max_bind_params: 0,
        }
    }

    /// Build the request body for a SQL query.
    pub fn build_request(&self, sql: &str, params: &[Value]) -> Value {
        match self.request_format {
            RequestFormat::Turso => {
                if params.is_empty() {
                    serde_json::json!({
                        "requests": [
                            {"type": "execute", "stmt": {"sql": sql}}
                        ]
                    })
                } else {
                    let args: Vec<Value> = params
                        .iter()
                        .map(|p| {
                            if let Some(s) = p.as_str() {
                                serde_json::json!({"type": "text", "value": s})
                            } else if let Some(n) = p.as_i64() {
                                serde_json::json!({"type": "integer", "value": n.to_string()})
                            } else if let Some(n) = p.as_f64() {
                                serde_json::json!({"type": "float", "value": n})
                            } else if p.is_null() {
                                serde_json::json!({"type": "null"})
                            } else {
                                serde_json::json!({"type": "text", "value": p.to_string()})
                            }
                        })
                        .collect();
                    serde_json::json!({
                        "requests": [
                            {"type": "execute", "stmt": {"sql": sql, "args": args}}
                        ]
                    })
                }
            }
            RequestFormat::Rqlite => {
                if params.is_empty() {
                    serde_json::json!([[sql]])
                } else {
                    let mut stmt = vec![Value::String(sql.to_string())];
                    stmt.extend(params.iter().cloned());
                    serde_json::json!([stmt])
                }
            }
            RequestFormat::D1 => {
                if params.is_empty() {
                    serde_json::json!({"sql": sql})
                } else {
                    serde_json::json!({"sql": sql, "params": params})
                }
            }
            RequestFormat::Datasette => {
                serde_json::json!({"sql": sql, "_shape": "array"})
            }
            RequestFormat::Generic => {
                if params.is_empty() {
                    serde_json::json!({"sql": sql})
                } else {
                    serde_json::json!({"sql": sql, "params": params})
                }
            }
        }
    }

    /// Navigate a JSON path to extract a nested value.
    pub fn extract_path<'a>(root: &'a Value, path: &[String]) -> Option<&'a Value> {
        let mut current = root;
        for segment in path {
            if let Ok(idx) = segment.parse::<usize>() {
                current = current.get(idx)?;
            } else {
                current = current.get(segment.as_str())?;
            }
        }
        Some(current)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_turso_request_no_params() {
        let p = Profile::turso();
        let body = p.build_request("SELECT 1", &[]);
        assert!(body["requests"][0]["stmt"]["sql"]
            .as_str()
            .unwrap()
            .contains("SELECT 1"));
    }

    #[test]
    fn test_turso_request_with_params() {
        let p = Profile::turso();
        let body = p.build_request(
            "SELECT * FROM users WHERE id = ?",
            &[Value::String("42".into())],
        );
        let args = &body["requests"][0]["stmt"]["args"];
        assert_eq!(args[0]["type"], "text");
        assert_eq!(args[0]["value"], "42");
    }

    #[test]
    fn test_rqlite_request() {
        let p = Profile::rqlite();
        let body = p.build_request("SELECT 1", &[]);
        assert_eq!(body[0][0], "SELECT 1");
    }

    #[test]
    fn test_generic_request() {
        let p = Profile::generic();
        let body = p.build_request("SELECT 1", &[]);
        assert_eq!(body["sql"], "SELECT 1");
    }

    #[test]
    fn test_extract_path_simple() {
        let json = serde_json::json!({"rows": [{"id": 1}]});
        let result = Profile::extract_path(&json, &["rows".into()]);
        assert!(result.unwrap().is_array());
    }

    #[test]
    fn test_extract_path_nested() {
        let json = serde_json::json!({"results": [{"response": {"result": {"rows": [1,2,3]}}}]});
        let path = vec![
            "results".into(),
            "0".into(),
            "response".into(),
            "result".into(),
            "rows".into(),
        ];
        let result = Profile::extract_path(&json, &path).unwrap();
        assert_eq!(result.as_array().unwrap().len(), 3);
    }

    #[test]
    fn test_d1_request() {
        let p = Profile::d1();
        let body = p.build_request("SELECT 1", &[]);
        assert_eq!(body["sql"], "SELECT 1");
    }

    #[test]
    fn test_datasette_request() {
        let p = Profile::datasette();
        let body = p.build_request("SELECT 1", &[]);
        assert_eq!(body["sql"], "SELECT 1");
        assert_eq!(body["_shape"], "array");
    }

    #[test]
    fn test_profile_from_name() {
        assert!(Profile::from_name("turso").is_some());
        assert!(Profile::from_name("libsql").is_some());
        assert!(Profile::from_name("rqlite").is_some());
        assert!(Profile::from_name("d1").is_some());
        assert!(Profile::from_name("cloudflare-d1").is_some());
        assert!(Profile::from_name("datasette").is_some());
        assert!(Profile::from_name("sqlite-cloud").is_some());
        assert!(Profile::from_name("sqlitecloud").is_some());
        assert!(Profile::from_name("starbasedb").is_some());
        assert!(Profile::from_name("starbase").is_some());
        assert!(Profile::from_name("generic").is_some());
        assert!(Profile::from_name("unknown").is_none());
    }
}

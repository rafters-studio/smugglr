//! Table name validation for SQL injection prevention.
//!
//! [`TableSchema::validate`] checks a table name against an allowlist of names
//! read from the live database schema and rejects anything not present. Callers
//! validate before interpolating a table name into SQL, so injection via the
//! table identifier is prevented by the allowlist check.

use crate::error::{Result, SyncError};
use std::collections::HashSet;

/// Schema information used for table name validation.
///
/// Holds the set of valid table names queried from the database.
#[derive(Debug, Clone)]
pub struct TableSchema {
    tables: HashSet<String>,
}

impl TableSchema {
    /// Create a new schema from an iterator of table names.
    pub fn new(tables: impl IntoIterator<Item = String>) -> Self {
        Self {
            tables: tables.into_iter().collect(),
        }
    }

    /// Validate a table name against this schema.
    ///
    /// # Errors
    ///
    /// Returns [`SyncError::InvalidTableName`] if the name is not in the allowlist.
    pub fn validate(&self, name: &str) -> Result<()> {
        if self.tables.contains(name) {
            return Ok(());
        }
        let mut available: Vec<_> = self.tables.iter().cloned().collect();
        available.sort();
        Err(SyncError::InvalidTableName {
            name: name.to_string(),
            available: available.join(", "),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> TableSchema {
        TableSchema::new(vec![
            "users".to_string(),
            "posts".to_string(),
            "comments".to_string(),
        ])
    }

    #[test]
    fn validate_existing_table() {
        let schema = test_schema();
        assert!(schema.validate("users").is_ok());
    }

    #[test]
    fn reject_nonexistent_table() {
        let schema = test_schema();
        let result = schema.validate("nonexistent");
        assert!(result.is_err());

        let err = result.unwrap_err();
        match err {
            SyncError::InvalidTableName { name, available } => {
                assert_eq!(name, "nonexistent");
                assert!(available.contains("users"));
                assert!(available.contains("posts"));
                assert!(available.contains("comments"));
            }
            _ => panic!("Expected InvalidTableName error"),
        }
    }

    #[test]
    fn reject_sql_injection_attempts() {
        let schema = test_schema();

        let injection_attempts = vec![
            "users; DROP TABLE users;--",
            "users' OR '1'='1",
            "users\" OR \"1\"=\"1",
            "users/**/UNION/**/SELECT/**/password/**/FROM/**/admin",
            "users; DELETE FROM users WHERE 1=1;--",
            "../../../etc/passwd",
        ];

        for attempt in injection_attempts {
            let result = schema.validate(attempt);
            assert!(
                result.is_err(),
                "Should reject injection attempt: {}",
                attempt
            );
        }
    }

    #[test]
    fn empty_schema() {
        let schema = TableSchema::new(Vec::<String>::new());
        assert!(schema.validate("anything").is_err());
    }

    #[test]
    fn table_names_are_case_sensitive() {
        let schema = test_schema();
        // SQLite table names are case-sensitive by default
        assert!(schema.validate("USERS").is_err());
        assert!(schema.validate("Users").is_err());
        assert!(schema.validate("users").is_ok());
    }
}

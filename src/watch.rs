//! Watch daemon for continuous sync
//!
//! Runs `sync_all` on a configurable interval with a `last_sync` cursor
//! persisted in the config file. First tick always does a full sync;
//! subsequent ticks reuse the same engine (the cursor is informational
//! for the user, not an optimization gate in v1).

use crate::config::{Config, ResolvedTarget};
use crate::error::{Result, SyncError};
use crate::local::LocalDb;
use crate::remote::D1Client;
use crate::sync::sync_all;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::signal;
use tokio::time::{self, Duration};
use tracing::{error, info, warn};

/// PID lock to prevent multiple daemon instances.
#[derive(Debug)]
pub struct PidLock {
    path: PathBuf,
}

impl PidLock {
    /// Acquire a PID lock. Returns an error if another instance is running.
    pub fn acquire(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        if path.exists() {
            let contents = fs::read_to_string(&path)?;
            if let Ok(pid) = contents.trim().parse::<u32>() {
                if is_process_running(pid) {
                    return Err(SyncError::Config(format!(
                        "Another smuggler watch instance is running (PID {}). \
                         If this is stale, remove {}",
                        pid,
                        path.display()
                    )));
                }
                warn!("Removing stale PID file (PID {} is not running)", pid);
            }
        }

        let pid = std::process::id();
        fs::write(&path, pid.to_string())?;
        info!("Acquired PID lock: {} (PID {})", path.display(), pid);

        Ok(Self { path })
    }

    /// Release the PID lock.
    pub fn release(&self) {
        if self.path.exists() {
            if let Err(e) = fs::remove_file(&self.path) {
                warn!("Failed to remove PID file: {}", e);
            } else {
                info!("Released PID lock: {}", self.path.display());
            }
        }
    }
}

impl Drop for PidLock {
    fn drop(&mut self) {
        self.release();
    }
}

/// Check if a process with the given PID is running.
fn is_process_running(pid: u32) -> bool {
    // On Unix, kill(pid, 0) checks existence without sending a signal
    #[cfg(unix)]
    {
        unsafe { libc::kill(pid as i32, 0) == 0 }
    }
    // On Windows, use the command-based check
    #[cfg(windows)]
    {
        std::process::Command::new("tasklist")
            .args(["/FI", &format!("PID eq {}", pid), "/NH"])
            .output()
            .map(|o| {
                let stdout = String::from_utf8_lossy(&o.stdout);
                stdout.contains(&pid.to_string())
            })
            .unwrap_or(false)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = pid;
        false
    }
}

/// Resolve the PID lock file path.
///
/// Uses `~/.smuggler/smuggler.pid` or a path next to the config file.
pub fn pid_lock_path(config_path: &Path) -> PathBuf {
    if let Some(parent) = config_path.parent() {
        if parent.as_os_str().is_empty() {
            PathBuf::from(".smuggler.pid")
        } else {
            parent.join(".smuggler.pid")
        }
    } else {
        PathBuf::from(".smuggler.pid")
    }
}

/// Update the `last_sync` timestamp in the config file.
///
/// Reads the file, updates or inserts the `last_sync` field under `[sync]`,
/// and writes it back. Uses string manipulation to preserve comments and
/// formatting in the user's TOML.
pub fn update_last_sync(config_path: &Path, timestamp: &str) -> Result<()> {
    let content = fs::read_to_string(config_path)?;
    let new_content = set_last_sync_in_toml(&content, timestamp);
    fs::write(config_path, new_content)?;
    info!("Updated last_sync to {}", timestamp);
    Ok(())
}

/// Insert or update `last_sync` in a TOML string.
fn set_last_sync_in_toml(content: &str, timestamp: &str) -> String {
    let last_sync_line = format!("last_sync = \"{}\"", timestamp);
    let mut lines: Vec<String> = content.lines().map(String::from).collect();

    // Look for existing last_sync line
    for line in &mut lines {
        let trimmed = line.trim();
        if trimmed.starts_with("last_sync") && trimmed.contains('=') {
            *line = last_sync_line;
            return lines.join("\n") + "\n";
        }
    }

    // No existing last_sync -- add it under [sync] section
    let mut found_sync_section = false;
    let mut insert_idx = None;

    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if trimmed == "[sync]" {
            found_sync_section = true;
            continue;
        }
        if found_sync_section {
            // Insert after last key in [sync] section, or right after [sync]
            if trimmed.starts_with('[') || trimmed.is_empty() {
                insert_idx = Some(i);
                break;
            }
        }
    }

    if let Some(idx) = insert_idx {
        lines.insert(idx, last_sync_line);
    } else if found_sync_section {
        // [sync] was the last section, append to end
        lines.push(last_sync_line);
    } else {
        // No [sync] section at all, add one
        lines.push(String::new());
        lines.push("[sync]".to_string());
        lines.push(last_sync_line);
    }

    lines.join("\n") + "\n"
}

/// Get the current UTC timestamp in ISO 8601 format.
fn now_iso8601() -> String {
    use std::time::SystemTime;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    // Format as ISO 8601 without external dep
    let secs = now.as_secs();
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Simple epoch-to-date conversion
    let (year, month, day) = epoch_days_to_date(days);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, day, hours, minutes, seconds
    )
}

/// Convert days since Unix epoch to (year, month, day).
fn epoch_days_to_date(days: u64) -> (u64, u64, u64) {
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Run the watch daemon loop.
pub async fn run_watch(
    config: &Config,
    config_path: &Path,
    target: ResolvedTarget,
    interval_secs: u64,
    dry_run: bool,
) -> Result<()> {
    let pid_path = pid_lock_path(config_path);
    let _pid_lock = PidLock::acquire(&pid_path)?;

    info!(
        "Starting watch daemon (interval: {}s, dry_run: {})",
        interval_secs, dry_run
    );

    let mut tick_count: u64 = 0;
    let mut interval = time::interval(Duration::from_secs(interval_secs));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                tick_count += 1;
                info!("Watch tick #{}", tick_count);

                let result = match &target {
                    ResolvedTarget::D1 { account_id, database_id, api_token } => {
                        let local = LocalDb::open(config.local_db_path())?;
                        let remote = D1Client::with_retry_config(
                            account_id.clone(),
                            database_id.clone(),
                            api_token.clone(),
                            config.retry_config(),
                        );
                        if let Err(e) = remote.test_connection().await {
                            warn!("Connection test failed on tick #{}: {}. Will retry next tick.", tick_count, e);
                            continue;
                        }
                        sync_all(&local, &remote, config, None, dry_run).await
                    }
                    ResolvedTarget::Sqlite { database } => {
                        let local = LocalDb::open(config.local_db_path())?;
                        let target_db = LocalDb::open(database)?;
                        sync_all(&local, &target_db, config, None, dry_run).await
                    }
                };

                match result {
                    Ok(results) => {
                        let total_pushed: usize = results.iter().map(|r| r.rows_pushed).sum();
                        let total_pulled: usize = results.iter().map(|r| r.rows_pulled).sum();

                        if total_pushed > 0 || total_pulled > 0 {
                            info!(
                                "Tick #{}: {} pushed, {} pulled across {} tables",
                                tick_count, total_pushed, total_pulled, results.len()
                            );
                        } else {
                            info!("Tick #{}: no changes", tick_count);
                        }

                        if !dry_run {
                            let ts = now_iso8601();
                            if let Err(e) = update_last_sync(config_path, &ts) {
                                warn!("Failed to update last_sync: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        if is_transient_error(&e) {
                            warn!("Transient error on tick #{}: {}. Will retry next tick.", tick_count, e);
                        } else {
                            error!("Fatal error on tick #{}: {}", tick_count, e);
                            return Err(e);
                        }
                    }
                }
            }
            _ = signal::ctrl_c() => {
                info!("Received shutdown signal. Stopping watch daemon.");
                break;
            }
        }
    }

    info!("Watch daemon stopped after {} ticks", tick_count);
    Ok(())
}

/// Check if an error is transient (should retry on next tick) vs fatal (should exit).
fn is_transient_error(err: &SyncError) -> bool {
    matches!(
        err,
        SyncError::RateLimited { .. }
            | SyncError::ServerError { .. }
            | SyncError::ConnectionTimeout
            | SyncError::Http(_)
            | SyncError::ConcurrentWrite
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_pid_lock_acquire_release() {
        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("test.pid");

        let lock = PidLock::acquire(&pid_path).unwrap();
        assert!(pid_path.exists());

        let contents = fs::read_to_string(&pid_path).unwrap();
        assert_eq!(contents, std::process::id().to_string());

        lock.release();
        assert!(!pid_path.exists());
    }

    #[test]
    fn test_pid_lock_stale_removed() {
        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("test.pid");

        // Write a PID that definitely doesn't exist
        fs::write(&pid_path, "99999999").unwrap();

        // Should succeed by removing stale lock
        let lock = PidLock::acquire(&pid_path).unwrap();
        let contents = fs::read_to_string(&pid_path).unwrap();
        assert_eq!(contents, std::process::id().to_string());
        lock.release();
    }

    #[test]
    fn test_pid_lock_self_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("test.pid");

        // Write our own PID -- should detect as running
        fs::write(&pid_path, std::process::id().to_string()).unwrap();

        let result = PidLock::acquire(&pid_path);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Another smuggler watch instance"));
    }

    #[test]
    fn test_pid_lock_drop_releases() {
        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("test.pid");

        {
            let _lock = PidLock::acquire(&pid_path).unwrap();
            assert!(pid_path.exists());
        }
        // Drop should have released
        assert!(!pid_path.exists());
    }

    #[test]
    fn test_set_last_sync_insert_under_sync() {
        let input = r#"local_db = "game.db"

[sync]
tables = ["abilities"]

[target]
type = "sqlite"
database = "backup.db"
"#;
        let result = set_last_sync_in_toml(input, "2026-03-31T12:00:00Z");
        assert!(result.contains("last_sync = \"2026-03-31T12:00:00Z\""));
        // Should be under [sync], before [target]
        let sync_pos = result.find("[sync]").unwrap();
        let last_sync_pos = result.find("last_sync").unwrap();
        let target_pos = result.find("[target]").unwrap();
        assert!(last_sync_pos > sync_pos);
        assert!(last_sync_pos < target_pos);
    }

    #[test]
    fn test_set_last_sync_update_existing() {
        let input = r#"local_db = "game.db"

[sync]
last_sync = "2026-03-30T00:00:00Z"
tables = ["abilities"]
"#;
        let result = set_last_sync_in_toml(input, "2026-03-31T12:00:00Z");
        assert!(result.contains("last_sync = \"2026-03-31T12:00:00Z\""));
        // Should not have duplicate
        assert_eq!(result.matches("last_sync").count(), 1);
    }

    #[test]
    fn test_set_last_sync_no_sync_section() {
        let input = r#"local_db = "game.db"

[target]
type = "sqlite"
database = "backup.db"
"#;
        let result = set_last_sync_in_toml(input, "2026-03-31T12:00:00Z");
        assert!(result.contains("[sync]"));
        assert!(result.contains("last_sync = \"2026-03-31T12:00:00Z\""));
    }

    #[test]
    fn test_update_last_sync_file() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(
            f,
            r#"local_db = "game.db"

[sync]
tables = ["abilities"]

[target]
type = "sqlite"
database = "backup.db""#
        )
        .unwrap();

        update_last_sync(f.path(), "2026-03-31T15:30:00Z").unwrap();

        let content = fs::read_to_string(f.path()).unwrap();
        assert!(content.contains("last_sync = \"2026-03-31T15:30:00Z\""));
    }

    #[test]
    fn test_now_iso8601_format() {
        let ts = now_iso8601();
        // Should match YYYY-MM-DDTHH:MM:SSZ pattern
        assert_eq!(ts.len(), 20);
        assert!(ts.ends_with('Z'));
        assert_eq!(&ts[4..5], "-");
        assert_eq!(&ts[7..8], "-");
        assert_eq!(&ts[10..11], "T");
        assert_eq!(&ts[13..14], ":");
        assert_eq!(&ts[16..17], ":");
    }

    #[test]
    fn test_epoch_days_to_date_known() {
        // 2026-03-31 = day 20543 since epoch (verified)
        let (y, m, d) = epoch_days_to_date(0);
        assert_eq!((y, m, d), (1970, 1, 1));

        // 2000-01-01 = day 10957
        let (y, m, d) = epoch_days_to_date(10957);
        assert_eq!((y, m, d), (2000, 1, 1));
    }

    #[test]
    fn test_pid_lock_path_with_config() {
        let p = pid_lock_path(Path::new("/home/user/project/config.toml"));
        assert_eq!(p, PathBuf::from("/home/user/project/.smuggler.pid"));
    }

    #[test]
    fn test_pid_lock_path_bare_filename() {
        let p = pid_lock_path(Path::new("config.toml"));
        assert_eq!(p, PathBuf::from(".smuggler.pid"));
    }

    #[test]
    fn test_is_transient_error() {
        assert!(is_transient_error(&SyncError::RateLimited {
            retry_after: None
        }));
        assert!(is_transient_error(&SyncError::ServerError {
            status: 503,
            message: "down".into()
        }));
        assert!(is_transient_error(&SyncError::ConnectionTimeout));
        assert!(is_transient_error(&SyncError::ConcurrentWrite));

        assert!(!is_transient_error(&SyncError::Config("bad".into())));
        assert!(!is_transient_error(&SyncError::TableNotFound("x".into())));
    }
}

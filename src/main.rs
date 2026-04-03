//! # Smuggler
//!
//! Smuggle data between SQLite and Cloudflare D1.
//!
//! A fast, stateless CLI tool for bidirectional synchronization between
//! local SQLite databases and Cloudflare D1.
//!
//! ## Features
//!
//! - **True change detection** - Compares actual row content via SHA256 hashing
//! - **Delta sync** - Only transfers rows that differ
//! - **Bidirectional** - Push, pull, or both with configurable conflict resolution
//! - **No state files** - Every run compares live data
//!
//! ## Architecture
//!
//! - [`config`] - Configuration loading from TOML
//! - [`local`] - Local SQLite database operations
//! - [`remote`] - Cloudflare D1 HTTP API client
//! - [`diff`] - Change detection algorithm
//! - [`sync`] - Push/pull orchestration
//! - [`error`] - Error types
//! - [`table`] - Table name validation
//! - [`datasource`] - DataSource trait for abstracting database backends
//! - [`batch`] - Batch operations for multi-row upserts

mod batch;
mod broadcast;
mod config;
mod datasource;
mod diff;
mod error;
mod local;
mod output;
mod remote;
mod stash;
mod sync;
mod table;
mod watch;

use crate::config::{Config, ResolvedTarget};
use crate::datasource::DataSource;
use crate::diff::diff_table;
use crate::local::LocalDb;
use crate::output::{
    CommandOutput, DiffOutput, DryRunOutput, DryRunTableOutput, DryRunVerboseTableOutput,
    ErrorOutput, OutputFormat, StatusConfig, StatusDb, StatusOutput, StatusTable,
};
use crate::remote::D1Client;
use crate::sync::{get_tables_to_sync, pull_all, push_all, sync_all};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
#[command(name = "smuggler")]
#[command(
    author,
    version,
    about = "Smuggle data between SQLite and Cloudflare D1"
)]
struct Cli {
    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,

    /// Output format: text (default) or json
    #[arg(short, long, default_value = "text")]
    output: OutputFormat,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Push local changes to D1 (local -> remote)
    Push {
        /// Specific table to push (default: all configured tables)
        #[arg(short, long)]
        table: Option<String>,

        /// Show what would be pushed without actually pushing
        #[arg(long)]
        dry_run: bool,
    },

    /// Pull remote changes to local (D1 -> local)
    Pull {
        /// Specific table to pull (default: all configured tables)
        #[arg(short, long)]
        table: Option<String>,

        /// Show what would be pulled without actually pulling
        #[arg(long)]
        dry_run: bool,
    },

    /// Show differences between local and remote
    Diff {
        /// Specific table to diff (default: all configured tables)
        #[arg(short, long)]
        table: Option<String>,
    },

    /// Show configuration and connection status
    Status,

    /// Bidirectional sync (push + pull in one operation)
    Sync {
        /// Specific table to sync (default: all configured tables)
        #[arg(short, long)]
        table: Option<String>,

        /// Show what would be synced without actually syncing
        #[arg(long)]
        dry_run: bool,
    },

    /// Stash local state to an S3-compatible relay (local -> S3)
    Stash {
        /// Specific table to stash (default: all configured tables)
        #[arg(short, long)]
        table: Option<String>,

        /// Show what would be stashed without actually uploading
        #[arg(long)]
        dry_run: bool,
    },

    /// Retrieve state from an S3-compatible relay (S3 -> local)
    Retrieve {
        /// Specific table to retrieve (default: all configured tables)
        #[arg(short, long)]
        table: Option<String>,

        /// Show what would be retrieved without actually applying
        #[arg(long)]
        dry_run: bool,
    },

    /// Watch for changes and sync periodically (daemon mode)
    Watch {
        /// Sync interval in seconds
        #[arg(short, long, default_value = "30")]
        interval: u64,

        /// Show what would be synced without actually syncing
        #[arg(long)]
        dry_run: bool,
    },

    /// LAN broadcast sync with peer discovery
    Broadcast {
        /// Override broadcast port
        #[arg(short = 'p', long)]
        port: Option<u16>,

        /// Sync interval in seconds
        #[arg(short, long)]
        interval: Option<u64>,

        /// Run a single sync cycle and exit
        #[arg(long)]
        once: bool,

        /// Show what would sync without applying
        #[arg(long)]
        dry_run: bool,
    },
}

/// Print a JSON error and exit with the appropriate code.
fn exit_json_error(command: &'static str, err: &error::SyncError) -> ! {
    let out = ErrorOutput {
        command,
        status: "error",
        error: err.to_string(),
        exit_code: err.exit_code(),
    };
    println!("{}", serde_json::to_string(&out).unwrap());
    std::process::exit(err.exit_code());
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let fmt = cli.output;

    // Set up logging -- suppress tracing output in JSON mode so stdout is clean
    let level = match fmt {
        OutputFormat::Json => Level::WARN,
        OutputFormat::Text if cli.verbose => Level::DEBUG,
        OutputFormat::Text => Level::INFO,
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .with_target(false)
        .with_writer(std::io::stderr)
        .compact()
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    // Determine command name for JSON output
    let command_name: &'static str = match &cli.command {
        Commands::Push { .. } => "push",
        Commands::Pull { .. } => "pull",
        Commands::Sync { .. } => "sync",
        Commands::Diff { .. } => "diff",
        Commands::Status => "status",
        Commands::Stash { .. } => "stash",
        Commands::Retrieve { .. } => "retrieve",
        Commands::Watch { .. } => "watch",
        Commands::Broadcast { .. } => "broadcast",
    };

    // Load config
    let config = match Config::load(&cli.config) {
        Ok(c) => c,
        Err(e) => {
            match fmt {
                OutputFormat::Json => exit_json_error(command_name, &e),
                OutputFormat::Text => {
                    error!("Failed to load config from {}: {}", cli.config.display(), e)
                }
            }
            std::process::exit(e.exit_code());
        }
    };

    // Resolve target once upfront (stash/retrieve/broadcast don't need it)
    let config_path = cli.config.clone();
    let target = match &cli.command {
        Commands::Stash { .. } | Commands::Retrieve { .. } | Commands::Broadcast { .. } => None,
        _ => Some(config.resolve_target().unwrap_or_else(|e| {
            match fmt {
                OutputFormat::Json => exit_json_error(command_name, &e),
                OutputFormat::Text => error!("Failed to resolve target: {}", e),
            }
            std::process::exit(e.exit_code());
        })),
    };

    // Execute command
    let result = match cli.command {
        Commands::Push { table, dry_run } => {
            run_push(&config, target.unwrap(), table, dry_run, fmt, cli.verbose).await
        }
        Commands::Pull { table, dry_run } => {
            run_pull(&config, target.unwrap(), table, dry_run, fmt, cli.verbose).await
        }
        Commands::Sync { table, dry_run } => {
            run_sync(&config, target.unwrap(), table, dry_run, fmt, cli.verbose).await
        }
        Commands::Diff { table } => run_diff(&config, target.unwrap(), table, fmt).await,
        Commands::Status => run_status(&config, target.unwrap(), fmt).await,
        Commands::Stash { table, dry_run } => {
            run_stash(&config, table, dry_run, fmt, cli.verbose).await
        }
        Commands::Retrieve { table, dry_run } => {
            run_retrieve(&config, table, dry_run, fmt, cli.verbose).await
        }
        Commands::Watch { interval, dry_run } => {
            watch::run_watch(
                &config,
                &config_path,
                target.unwrap(),
                interval,
                dry_run,
                fmt,
            )
            .await
        }
        Commands::Broadcast {
            port,
            interval,
            once,
            dry_run,
        } => {
            let mut bc = config
                .broadcast
                .clone()
                .unwrap_or_else(broadcast::BroadcastConfig::default);
            if let Some(p) = port {
                bc.port = p;
            }
            if let Some(i) = interval {
                bc.interval_secs = i;
            }
            broadcast::run_broadcast(&config, &config_path, &bc, once, dry_run).await
        }
    };

    if let Err(e) = result {
        match fmt {
            OutputFormat::Json => exit_json_error(command_name, &e),
            OutputFormat::Text => error!("Error: {}", e),
        }
        std::process::exit(e.exit_code());
    }
}

fn print_dry_run_json(command: &'static str, results: &[sync::SyncResult], verbose: bool) {
    if verbose {
        let out = DryRunOutput::<DryRunVerboseTableOutput>::from_sync_results(command, results);
        println!("{}", serde_json::to_string(&out).unwrap());
    } else {
        let out = DryRunOutput::<DryRunTableOutput>::from_sync_results(command, results);
        println!("{}", serde_json::to_string(&out).unwrap());
    }
}

/// Open a D1Client from resolved target fields.
fn open_d1(account_id: &str, database_id: &str, api_token: &str, config: &Config) -> D1Client {
    D1Client::with_retry_config(
        account_id.to_string(),
        database_id.to_string(),
        api_token.to_string(),
        config.retry_config(),
    )
}

/// Resolve table filter from CLI --table arg using local schema validation.
fn resolve_tables(local: &LocalDb, table: Option<String>) -> error::Result<Option<Vec<String>>> {
    match table {
        Some(t) => {
            let schema = local.get_schema()?;
            let _ = schema.validate(&t)?;
            Ok(Some(vec![t]))
        }
        None => Ok(None),
    }
}

async fn run_push(
    config: &Config,
    target: ResolvedTarget,
    table: Option<String>,
    dry_run: bool,
    fmt: OutputFormat,
    verbose: bool,
) -> error::Result<()> {
    let local = LocalDb::open_readonly(config.local_db_path())?;
    let tables = resolve_tables(&local, table)?;

    let results = match target {
        ResolvedTarget::D1 {
            account_id,
            database_id,
            api_token,
        } => {
            info!("Push mode: local -> D1");
            let remote = open_d1(&account_id, &database_id, &api_token, config);
            remote.test_connection().await?;
            push_all(&local, &remote, config, tables, dry_run).await?
        }
        ResolvedTarget::Sqlite { database } => {
            info!("Push mode: local -> SQLite ({})", database);
            let target_db = LocalDb::open(&database)?;
            push_all(&local, &target_db, config, tables, dry_run).await?
        }
    };

    match fmt {
        OutputFormat::Json if dry_run => print_dry_run_json("push", &results, verbose),
        OutputFormat::Json => {
            let out = CommandOutput::from_sync_results("push", &results, false);
            println!("{}", serde_json::to_string(&out).unwrap());
        }
        OutputFormat::Text => {
            print_summary("Push", &results, |r| r.rows_pushed, "push", dry_run);
        }
    }
    Ok(())
}

async fn run_pull(
    config: &Config,
    target: ResolvedTarget,
    table: Option<String>,
    dry_run: bool,
    fmt: OutputFormat,
    verbose: bool,
) -> error::Result<()> {
    let local = if dry_run {
        LocalDb::open_readonly(config.local_db_path())?
    } else {
        LocalDb::open(config.local_db_path())?
    };
    let tables = resolve_tables(&local, table)?;

    let results = match target {
        ResolvedTarget::D1 {
            account_id,
            database_id,
            api_token,
        } => {
            info!("Pull mode: D1 -> local");
            let remote = open_d1(&account_id, &database_id, &api_token, config);
            remote.test_connection().await?;
            pull_all(&local, &remote, config, tables, dry_run).await?
        }
        ResolvedTarget::Sqlite { database } => {
            info!("Pull mode: SQLite ({}) -> local", database);
            let source_db = LocalDb::open_readonly(&database)?;
            pull_all(&local, &source_db, config, tables, dry_run).await?
        }
    };

    match fmt {
        OutputFormat::Json if dry_run => print_dry_run_json("pull", &results, verbose),
        OutputFormat::Json => {
            let out = CommandOutput::from_sync_results("pull", &results, false);
            println!("{}", serde_json::to_string(&out).unwrap());
        }
        OutputFormat::Text => {
            print_summary("Pull", &results, |r| r.rows_pulled, "pull", dry_run);
        }
    }
    Ok(())
}

async fn run_sync(
    config: &Config,
    target: ResolvedTarget,
    table: Option<String>,
    dry_run: bool,
    fmt: OutputFormat,
    verbose: bool,
) -> error::Result<()> {
    let local = if dry_run {
        LocalDb::open_readonly(config.local_db_path())?
    } else {
        LocalDb::open(config.local_db_path())?
    };
    let tables = resolve_tables(&local, table)?;

    let results = match target {
        ResolvedTarget::D1 {
            account_id,
            database_id,
            api_token,
        } => {
            info!("Sync mode: bidirectional (local <-> D1)");
            let remote = open_d1(&account_id, &database_id, &api_token, config);
            remote.test_connection().await?;
            sync_all(&local, &remote, config, tables, dry_run).await?
        }
        ResolvedTarget::Sqlite { database } => {
            info!("Sync mode: bidirectional (local <-> SQLite {})", database);
            let target_db = LocalDb::open(&database)?;
            sync_all(&local, &target_db, config, tables, dry_run).await?
        }
    };

    match fmt {
        OutputFormat::Json if dry_run => print_dry_run_json("sync", &results, verbose),
        OutputFormat::Json => {
            let out = CommandOutput::from_sync_results("sync", &results, false);
            println!("{}", serde_json::to_string(&out).unwrap());
        }
        OutputFormat::Text => {
            println!("\n--- Sync Summary ---");
            let mut total_pushed = 0;
            let mut total_pulled = 0;
            for result in &results {
                if result.has_changes() {
                    println!(
                        "  {}: {} pushed, {} pulled",
                        result.table, result.rows_pushed, result.rows_pulled
                    );
                    total_pushed += result.rows_pushed;
                    total_pulled += result.rows_pulled;
                }
            }
            if total_pushed == 0 && total_pulled == 0 {
                println!("  No changes to sync");
            } else if dry_run {
                println!("\n  (dry run - no actual changes made)");
            }
        }
    }

    Ok(())
}

async fn run_diff(
    config: &Config,
    target: ResolvedTarget,
    table: Option<String>,
    fmt: OutputFormat,
) -> error::Result<()> {
    info!("Computing differences...");
    let local = LocalDb::open_readonly(config.local_db_path())?;

    match target {
        ResolvedTarget::D1 {
            account_id,
            database_id,
            api_token,
        } => {
            let remote = open_d1(&account_id, &database_id, &api_token, config);
            remote.test_connection().await?;
            let tables = match table {
                Some(t) => {
                    let schema = local.get_schema()?;
                    let _ = schema.validate(&t)?;
                    vec![t]
                }
                None => get_tables_to_sync(&local, &remote, config).await?,
            };
            output_diffs(
                &local,
                &remote,
                &tables,
                &config.sync.timestamp_column,
                &config.sync.exclude_columns,
                fmt,
            )
            .await
        }
        ResolvedTarget::Sqlite { database } => {
            let target_db = LocalDb::open_readonly(&database)?;
            let tables = match table {
                Some(t) => {
                    let schema = local.get_schema()?;
                    let _ = schema.validate(&t)?;
                    vec![t]
                }
                None => get_tables_to_sync(&local, &target_db, config).await?,
            };
            output_diffs(
                &local,
                &target_db,
                &tables,
                &config.sync.timestamp_column,
                &config.sync.exclude_columns,
                fmt,
            )
            .await
        }
    }
}

async fn output_diffs<A: DataSource, B: DataSource>(
    local: &A,
    remote: &B,
    tables: &[String],
    timestamp_column: &str,
    exclude_columns: &[String],
    fmt: OutputFormat,
) -> error::Result<()> {
    let mut diffs = Vec::new();
    for table_name in tables {
        let diff = diff_table(local, remote, table_name, timestamp_column, exclude_columns).await?;
        diffs.push((table_name.clone(), diff));
    }

    match fmt {
        OutputFormat::Json => {
            let out = DiffOutput::from_diffs(diffs);
            println!("{}", serde_json::to_string(&out).unwrap());
        }
        OutputFormat::Text => {
            println!("\n--- Differences ---");
            let mut has_any_changes = false;

            for (table_name, diff) in &diffs {
                if diff.has_changes() {
                    has_any_changes = true;
                    println!("\n{}", table_name);
                    println!("  {}", diff.summary());

                    print_diff_category("Local only", &diff.local_only);
                    print_diff_category("Remote only", &diff.remote_only);
                    print_diff_category("Local newer", &diff.local_newer);
                    print_diff_category("Remote newer", &diff.remote_newer);
                    print_diff_category("Content differs", &diff.content_differs);
                } else {
                    println!("\n{}: in sync ({} rows)", table_name, diff.identical.len());
                }
            }

            if !has_any_changes {
                println!("\nAll tables are in sync!");
            }
        }
    }

    Ok(())
}

/// Print a sync summary (push, pull, stash, or retrieve).
///
/// `verb` is the lowercase action name used in the no-changes message
/// (e.g. "No changes to push").
fn print_summary(
    heading: &str,
    results: &[sync::SyncResult],
    get_count: impl Fn(&sync::SyncResult) -> usize,
    verb: &str,
    dry_run: bool,
) {
    println!("\n--- {} Summary ---", heading);
    let mut total = 0;
    for result in results {
        let count = get_count(result);
        if count > 0 {
            println!("  {}: {} rows", result.table, count);
            total += count;
        }
    }

    if total == 0 {
        println!("  No changes to {}", verb);
    } else if dry_run {
        println!("\n  (dry run - no actual changes made)");
    }
}

/// Print a diff category (e.g. "Local only") with up to 5 sample keys.
fn print_diff_category(label: &str, keys: &[String]) {
    if keys.is_empty() {
        return;
    }
    let preview: Vec<_> = keys.iter().take(5).map(String::as_str).collect();
    println!("    {}: {}", label, preview.join(", "));
    if keys.len() > 5 {
        println!("      ... and {} more", keys.len() - 5);
    }
}

async fn run_status(
    config: &Config,
    target: ResolvedTarget,
    fmt: OutputFormat,
) -> error::Result<()> {
    let target_type = match &target {
        ResolvedTarget::D1 { .. } => "d1",
        ResolvedTarget::Sqlite { .. } => "sqlite",
    };

    // Gather local DB info
    let local_status = match LocalDb::open_readonly(config.local_db_path()) {
        Ok(local) => {
            let tables = local.list_tables().await?;
            let mut table_rows = Vec::new();
            for table in &tables {
                if config.should_sync_table(table) {
                    let count = local.row_count(table).await?;
                    table_rows.push(StatusTable {
                        name: table.clone(),
                        rows: count,
                    });
                }
            }
            StatusDb {
                connected: true,
                error: None,
                tables: table_rows,
            }
        }
        Err(e) => StatusDb {
            connected: false,
            error: Some(e.to_string()),
            tables: vec![],
        },
    };

    // Gather target info
    let target_status = match &target {
        ResolvedTarget::D1 {
            account_id,
            database_id,
            api_token,
        } => {
            let remote = open_d1(account_id, database_id, api_token, config);
            match remote.test_connection().await {
                Ok(()) => {
                    let tables = remote.list_tables().await?;
                    let mut table_rows = Vec::new();
                    for table in &tables {
                        if config.should_sync_table(table) {
                            let count = remote.row_count(table).await?;
                            table_rows.push(StatusTable {
                                name: table.clone(),
                                rows: count,
                            });
                        }
                    }
                    StatusDb {
                        connected: true,
                        error: None,
                        tables: table_rows,
                    }
                }
                Err(e) => StatusDb {
                    connected: false,
                    error: Some(e.to_string()),
                    tables: vec![],
                },
            }
        }
        ResolvedTarget::Sqlite { database } => match LocalDb::open_readonly(database) {
            Ok(target_db) => {
                let tables = target_db.list_tables().await?;
                let mut table_rows = Vec::new();
                for table in &tables {
                    if config.should_sync_table(table) {
                        let count = target_db.row_count(table).await?;
                        table_rows.push(StatusTable {
                            name: table.clone(),
                            rows: count,
                        });
                    }
                }
                StatusDb {
                    connected: true,
                    error: None,
                    tables: table_rows,
                }
            }
            Err(e) => StatusDb {
                connected: false,
                error: Some(e.to_string()),
                tables: vec![],
            },
        },
    };

    match fmt {
        OutputFormat::Json => {
            let out = StatusOutput {
                command: "status",
                status: "ok",
                config: StatusConfig {
                    local_db: config.local_db_path().to_string(),
                    target_type: target_type.to_string(),
                    timestamp_column: config.sync.timestamp_column.clone(),
                    conflict_resolution: format!("{:?}", config.sync.conflict_resolution),
                    tables: config.sync.tables.clone(),
                    exclude_tables: config.sync.exclude_tables.clone(),
                },
                local: local_status,
                target: target_status,
            };
            println!("{}", serde_json::to_string(&out).unwrap());
        }
        OutputFormat::Text => {
            println!("--- Configuration ---");
            println!("  Config file: loaded");
            println!("  Local DB: {}", config.local_db_path());

            match &target {
                ResolvedTarget::D1 {
                    account_id,
                    database_id,
                    ..
                } => {
                    println!("  Target: D1");
                    println!(
                        "  Account ID: {}...",
                        &account_id[..8.min(account_id.len())]
                    );
                    println!(
                        "  Database ID: {}...",
                        &database_id[..8.min(database_id.len())]
                    );
                }
                ResolvedTarget::Sqlite { database } => {
                    println!("  Target: SQLite ({})", database);
                }
            }

            println!("  Timestamp column: {}", config.sync.timestamp_column);
            println!(
                "  Conflict resolution: {:?}",
                config.sync.conflict_resolution
            );

            if !config.sync.tables.is_empty() {
                println!("  Tables (explicit): {}", config.sync.tables.join(", "));
            }
            if !config.sync.exclude_tables.is_empty() {
                println!(
                    "  Excluded tables: {}",
                    config.sync.exclude_tables.join(", ")
                );
            }

            // Local DB
            println!("\n--- Local Database ---");
            if local_status.connected {
                println!("  Connection: OK");
                println!("  Tables: {}", local_status.tables.len());
                for t in &local_status.tables {
                    println!("    {}: {} rows", t.name, t.rows);
                }
            } else {
                println!(
                    "  Connection: FAILED - {}",
                    local_status.error.as_deref().unwrap_or("unknown")
                );
            }

            // Target
            match &target {
                ResolvedTarget::D1 { .. } => println!("\n--- Remote D1 ---"),
                ResolvedTarget::Sqlite { .. } => println!("\n--- Target SQLite ---"),
            }
            if target_status.connected {
                println!("  Connection: OK");
                println!("  Tables: {}", target_status.tables.len());
                for t in &target_status.tables {
                    println!("    {}: {} rows", t.name, t.rows);
                }
            } else {
                println!(
                    "  Connection: FAILED - {}",
                    target_status.error.as_deref().unwrap_or("unknown")
                );
            }
        }
    }

    Ok(())
}

fn require_stash_config(config: &Config) -> error::Result<&config::StashConfig> {
    config
        .stash
        .as_ref()
        .ok_or_else(|| error::SyncError::Config("No [stash] section in config".into()))
}

async fn run_stash(
    config: &Config,
    table: Option<String>,
    dry_run: bool,
    fmt: OutputFormat,
    verbose: bool,
) -> error::Result<()> {
    let stash_config = require_stash_config(config)?;
    info!("Stash mode: local -> S3 relay");

    let results = stash::stash(
        stash_config,
        config.local_db_path(),
        &config.sync.timestamp_column,
        config.sync.conflict_resolution,
        table,
        dry_run,
        &config.sync.exclude_tables,
    )
    .await?;

    match fmt {
        OutputFormat::Json if dry_run => print_dry_run_json("stash", &results, verbose),
        OutputFormat::Json => {
            let out = CommandOutput::from_sync_results("stash", &results, false);
            println!("{}", serde_json::to_string(&out).unwrap());
        }
        OutputFormat::Text => {
            print_summary("Stash", &results, |r| r.rows_pushed, "stash", dry_run);
        }
    }
    Ok(())
}

async fn run_retrieve(
    config: &Config,
    table: Option<String>,
    dry_run: bool,
    fmt: OutputFormat,
    verbose: bool,
) -> error::Result<()> {
    let stash_config = require_stash_config(config)?;
    info!("Retrieve mode: S3 relay -> local");

    let results = stash::retrieve(
        stash_config,
        config.local_db_path(),
        &config.sync.timestamp_column,
        config.sync.conflict_resolution,
        table,
        dry_run,
        &config.sync.exclude_tables,
    )
    .await?;

    match fmt {
        OutputFormat::Json if dry_run => print_dry_run_json("retrieve", &results, verbose),
        OutputFormat::Json => {
            let out = CommandOutput::from_sync_results("retrieve", &results, false);
            println!("{}", serde_json::to_string(&out).unwrap());
        }
        OutputFormat::Text => {
            print_summary("Retrieve", &results, |r| r.rows_pulled, "retrieve", dry_run);
        }
    }
    Ok(())
}

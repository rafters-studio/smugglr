//! Smuggler CLI binary.
//!
//! This is the command-line interface for smuggler. All core sync logic
//! lives in `smugglr_core`; this crate provides the CLI argument parsing,
//! progress display, and human/JSON output formatting.

mod broadcast;
mod output;
mod watch;

use output::{
    CommandOutput, DiffOutput, DryRunOutput, DryRunTableOutput, DryRunVerboseTableOutput,
    ErrorOutput, OutputFormat, SnapshotListEntry, SnapshotListOutput, SnapshotOutput,
    SnapshotTableInfo, StatusConfig, StatusDb, StatusOutput, StatusTable,
};
use smugglr_core::config::{Config, ResolvedTarget};
use smugglr_core::datasource::DataSource;
use smugglr_core::diff::diff_table;
use smugglr_core::error;
use smugglr_core::local::LocalDb;
use smugglr_core::plugin::PluginDataSource;
use smugglr_core::sync::{
    get_tables_to_sync, pull_all, push_all, sync_all, NoProgress, SyncProgress,
};

use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::PathBuf;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

/// CLI progress reporter using indicatif progress bars.
struct IndicatifProgress {
    bar: std::sync::Mutex<Option<ProgressBar>>,
}

impl IndicatifProgress {
    fn new() -> Self {
        Self {
            bar: std::sync::Mutex::new(None),
        }
    }
}

impl SyncProgress for IndicatifProgress {
    fn on_transfer_start(&self, total_rows: usize, label: &str, table: &str) {
        let pb = ProgressBar::new(total_rows as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
                .expect("valid progress template"),
        );
        pb.set_message(format!("{} {}", label, table));
        *self.bar.lock().expect("mutex poisoned") = Some(pb);
    }

    fn on_batch_complete(&self, rows_in_batch: usize) {
        if let Some(ref pb) = *self.bar.lock().expect("mutex poisoned") {
            pb.inc(rows_in_batch as u64);
        }
    }

    fn on_transfer_finish(&self, total_rows: usize, label: &str) {
        if let Some(ref pb) = *self.bar.lock().expect("mutex poisoned") {
            pb.finish_with_message(format!("{} {} rows", label, total_rows));
        }
    }
}

fn make_progress(fmt: OutputFormat) -> Box<dyn SyncProgress> {
    if fmt == OutputFormat::Text {
        Box::new(IndicatifProgress::new())
    } else {
        Box::new(NoProgress)
    }
}

#[derive(Parser)]
#[command(name = "smugglr")]
#[command(
    author,
    version,
    about = "Smuggle data between SQLite-shaped things"
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
    /// Push local changes to the remote target (local -> remote)
    Push {
        /// Specific table to push (default: all configured tables)
        #[arg(short, long)]
        table: Option<String>,

        /// Show what would be pushed without actually pushing
        #[arg(long)]
        dry_run: bool,
    },

    /// Pull changes from the remote target to local (remote -> local)
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

    /// Create a point-in-time snapshot of the local database
    Snapshot {
        /// Show what would be snapshotted without uploading
        #[arg(long)]
        dry_run: bool,
    },

    /// List available snapshots
    Snapshots,

    /// Restore local database from a snapshot
    Restore {
        /// Timestamp of snapshot to restore (exact or closest before)
        timestamp: String,

        /// Show what would be restored without applying
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
    println!(
        "{}",
        serde_json::to_string(&out).expect("ErrorOutput serialization")
    );
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
        Commands::Snapshot { .. } => "snapshot",
        Commands::Snapshots => "snapshots",
        Commands::Restore { .. } => "restore",
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
        Commands::Stash { .. }
        | Commands::Retrieve { .. }
        | Commands::Snapshot { .. }
        | Commands::Snapshots
        | Commands::Restore { .. }
        | Commands::Broadcast { .. } => None,
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
            run_push(
                &config,
                target.expect("target resolved"),
                table,
                dry_run,
                fmt,
                cli.verbose,
            )
            .await
        }
        Commands::Pull { table, dry_run } => {
            run_pull(
                &config,
                target.expect("target resolved"),
                table,
                dry_run,
                fmt,
                cli.verbose,
            )
            .await
        }
        Commands::Sync { table, dry_run } => {
            run_sync(
                &config,
                target.expect("target resolved"),
                table,
                dry_run,
                fmt,
                cli.verbose,
            )
            .await
        }
        Commands::Diff { table } => {
            run_diff(&config, target.expect("target resolved"), table, fmt).await
        }
        Commands::Status => run_status(&config, target.expect("target resolved"), fmt).await,
        Commands::Stash { table, dry_run } => {
            run_stash(&config, table, dry_run, fmt, cli.verbose).await
        }
        Commands::Retrieve { table, dry_run } => {
            run_retrieve(&config, table, dry_run, fmt, cli.verbose).await
        }
        Commands::Snapshot { dry_run } => run_snapshot(&config, dry_run, fmt).await,
        Commands::Snapshots => run_snapshots(&config, fmt).await,
        Commands::Restore { timestamp, dry_run } => {
            run_restore(&config, &timestamp, dry_run, fmt).await
        }
        Commands::Watch { interval, dry_run } => {
            watch::run_watch(
                &config,
                &config_path,
                target.expect("target resolved"),
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
                .unwrap_or_else(smugglr_core::broadcast::BroadcastConfig::default);
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

fn print_dry_run_json(
    command: &'static str,
    results: &[smugglr_core::sync::SyncResult],
    verbose: bool,
) {
    if verbose {
        let out = DryRunOutput::<DryRunVerboseTableOutput>::from_sync_results(command, results);
        println!(
            "{}",
            serde_json::to_string(&out).expect("DryRunOutput serialization")
        );
    } else {
        let out = DryRunOutput::<DryRunTableOutput>::from_sync_results(command, results);
        println!(
            "{}",
            serde_json::to_string(&out).expect("DryRunOutput serialization")
        );
    }
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

    let progress = make_progress(fmt);

    let results = match target {
        ResolvedTarget::Sqlite { database } => {
            info!("Push mode: local -> SQLite ({})", database);
            let target_db = LocalDb::open(&database)?;
            push_all(
                &local,
                &target_db,
                config,
                tables,
                dry_run,
                progress.as_ref(),
            )
            .await?
        }
        ResolvedTarget::Plugin {
            ref path,
            ref name,
            config: ref plugin_config,
        } => {
            info!("Push mode: local -> plugin ({})", name);
            let plugin = PluginDataSource::start(path, name, plugin_config).await?;
            push_all(&local, &plugin, config, tables, dry_run, progress.as_ref()).await?
        }
    };

    match fmt {
        OutputFormat::Json if dry_run => print_dry_run_json("push", &results, verbose),
        OutputFormat::Json => {
            let out = CommandOutput::from_sync_results("push", &results, false);
            println!(
                "{}",
                serde_json::to_string(&out).expect("CommandOutput serialization")
            );
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

    let progress = make_progress(fmt);

    let results = match target {
        ResolvedTarget::Sqlite { database } => {
            info!("Pull mode: SQLite ({}) -> local", database);
            let source_db = LocalDb::open_readonly(&database)?;
            pull_all(
                &local,
                &source_db,
                config,
                tables,
                dry_run,
                progress.as_ref(),
            )
            .await?
        }
        ResolvedTarget::Plugin {
            ref path,
            ref name,
            config: ref plugin_config,
        } => {
            info!("Pull mode: plugin ({}) -> local", name);
            let plugin = PluginDataSource::start(path, name, plugin_config).await?;
            pull_all(&local, &plugin, config, tables, dry_run, progress.as_ref()).await?
        }
    };

    match fmt {
        OutputFormat::Json if dry_run => print_dry_run_json("pull", &results, verbose),
        OutputFormat::Json => {
            let out = CommandOutput::from_sync_results("pull", &results, false);
            println!(
                "{}",
                serde_json::to_string(&out).expect("CommandOutput serialization")
            );
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

    let progress = make_progress(fmt);

    let results = match target {
        ResolvedTarget::Sqlite { database } => {
            info!("Sync mode: bidirectional (local <-> SQLite {})", database);
            let target_db = LocalDb::open(&database)?;
            sync_all(
                &local,
                &target_db,
                config,
                tables,
                dry_run,
                progress.as_ref(),
            )
            .await?
        }
        ResolvedTarget::Plugin {
            ref path,
            ref name,
            config: ref plugin_config,
        } => {
            info!("Sync mode: bidirectional (local <-> plugin {})", name);
            let plugin = PluginDataSource::start(path, name, plugin_config).await?;
            sync_all(&local, &plugin, config, tables, dry_run, progress.as_ref()).await?
        }
    };

    match fmt {
        OutputFormat::Json if dry_run => print_dry_run_json("sync", &results, verbose),
        OutputFormat::Json => {
            let out = CommandOutput::from_sync_results("sync", &results, false);
            println!(
                "{}",
                serde_json::to_string(&out).expect("CommandOutput serialization")
            );
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
        ResolvedTarget::Plugin {
            ref path,
            ref name,
            config: ref plugin_config,
        } => {
            let plugin = PluginDataSource::start(path, name, plugin_config).await?;
            let tables = match table {
                Some(t) => {
                    let schema = local.get_schema()?;
                    let _ = schema.validate(&t)?;
                    vec![t]
                }
                None => get_tables_to_sync(&local, &plugin, config).await?,
            };
            output_diffs(
                &local,
                &plugin,
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
            println!(
                "{}",
                serde_json::to_string(&out).expect("DiffOutput serialization")
            );
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
    results: &[smugglr_core::sync::SyncResult],
    get_count: impl Fn(&smugglr_core::sync::SyncResult) -> usize,
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
        ResolvedTarget::Sqlite { .. } => "sqlite",
        ResolvedTarget::Plugin { ref name, .. } => name.as_str(),
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
        ResolvedTarget::Plugin {
            ref path,
            ref name,
            config: ref plugin_config,
        } => match PluginDataSource::start(path, name, plugin_config).await {
            Ok(plugin) => {
                let tables = plugin.list_tables().await?;
                let mut table_rows = Vec::new();
                for table in &tables {
                    if config.should_sync_table(table) {
                        let count = plugin.row_count(table).await?;
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
            println!(
                "{}",
                serde_json::to_string(&out).expect("StatusOutput serialization")
            );
        }
        OutputFormat::Text => {
            println!("--- Configuration ---");
            println!("  Config file: loaded");
            println!("  Local DB: {}", config.local_db_path());

            match &target {
                ResolvedTarget::Sqlite { database } => {
                    println!("  Target: SQLite ({})", database);
                }
                ResolvedTarget::Plugin {
                    ref name, ref path, ..
                } => {
                    println!("  Target: Plugin ({})", name);
                    println!("  Plugin path: {}", path.display());
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
                ResolvedTarget::Sqlite { .. } => println!("\n--- Target SQLite ---"),
                ResolvedTarget::Plugin { ref name, .. } => {
                    println!("\n--- Target Plugin ({}) ---", name)
                }
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

fn require_stash_config(config: &Config) -> error::Result<&smugglr_core::config::StashConfig> {
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

    let results = smugglr_core::stash::stash(
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
            println!(
                "{}",
                serde_json::to_string(&out).expect("CommandOutput serialization")
            );
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

    let results = smugglr_core::stash::retrieve(
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
            println!(
                "{}",
                serde_json::to_string(&out).expect("CommandOutput serialization")
            );
        }
        OutputFormat::Text => {
            print_summary("Retrieve", &results, |r| r.rows_pulled, "retrieve", dry_run);
        }
    }
    Ok(())
}

async fn run_snapshot(config: &Config, dry_run: bool, fmt: OutputFormat) -> error::Result<()> {
    let stash_config = require_stash_config(config)?;
    info!("Snapshot mode: local -> S3 relay");

    let result =
        smugglr_core::snapshot::snapshot(stash_config, config.local_db_path(), dry_run).await?;

    match fmt {
        OutputFormat::Json => {
            let out = SnapshotOutput {
                command: "snapshot",
                status: if dry_run { "dry_run" } else { "ok" },
                timestamp: result.timestamp,
                size_bytes: result.size_bytes,
                tables: result
                    .tables
                    .into_iter()
                    .map(|t| SnapshotTableInfo {
                        name: t.name,
                        row_count: t.row_count,
                    })
                    .collect(),
            };
            println!(
                "{}",
                serde_json::to_string(&out).expect("SnapshotOutput serialization")
            );
        }
        OutputFormat::Text => {
            println!("\n--- Snapshot ---");
            println!("  Timestamp: {}", result.timestamp);
            println!("  Size: {} bytes", result.size_bytes);
            for t in &result.tables {
                println!("  {}: {} rows", t.name, t.row_count);
            }
            if dry_run {
                println!("\n  (dry run - no snapshot created)");
            }
        }
    }
    Ok(())
}

async fn run_snapshots(config: &Config, fmt: OutputFormat) -> error::Result<()> {
    let stash_config = require_stash_config(config)?;
    info!("Listing snapshots");

    let entries = smugglr_core::snapshot::list_snapshots(stash_config).await?;

    match fmt {
        OutputFormat::Json => {
            let out = SnapshotListOutput {
                command: "snapshots",
                status: "ok",
                snapshots: entries
                    .into_iter()
                    .map(|e| SnapshotListEntry {
                        timestamp: e.timestamp,
                        size_bytes: e.size_bytes,
                        tables: e
                            .tables
                            .into_iter()
                            .map(|t| SnapshotTableInfo {
                                name: t.name,
                                row_count: t.row_count,
                            })
                            .collect(),
                    })
                    .collect(),
            };
            println!(
                "{}",
                serde_json::to_string(&out).expect("SnapshotListOutput serialization")
            );
        }
        OutputFormat::Text => {
            if entries.is_empty() {
                println!("No snapshots available");
            } else {
                println!("\n--- Snapshots ---");
                for entry in &entries {
                    let total_rows: usize = entry.tables.iter().map(|t| t.row_count).sum();
                    println!(
                        "  {} ({} bytes, {} tables, {} rows)",
                        entry.timestamp,
                        entry.size_bytes,
                        entry.tables.len(),
                        total_rows
                    );
                }
                println!("\n  {} snapshot(s) available", entries.len());
            }
        }
    }
    Ok(())
}

async fn run_restore(
    config: &Config,
    timestamp: &str,
    dry_run: bool,
    fmt: OutputFormat,
) -> error::Result<()> {
    let stash_config = require_stash_config(config)?;
    info!("Restore mode: S3 relay -> local");

    let result =
        smugglr_core::snapshot::restore(stash_config, config.local_db_path(), timestamp, dry_run)
            .await?;

    match fmt {
        OutputFormat::Json => {
            let out = SnapshotOutput {
                command: "restore",
                status: if dry_run { "dry_run" } else { "ok" },
                timestamp: result.timestamp,
                size_bytes: result.size_bytes,
                tables: result
                    .tables
                    .into_iter()
                    .map(|t| SnapshotTableInfo {
                        name: t.name,
                        row_count: t.row_count,
                    })
                    .collect(),
            };
            println!(
                "{}",
                serde_json::to_string(&out).expect("SnapshotOutput serialization")
            );
        }
        OutputFormat::Text => {
            println!("\n--- Restore ---");
            println!("  Restored snapshot: {}", result.timestamp);
            println!("  Size: {} bytes", result.size_bytes);
            for t in &result.tables {
                println!("  {}: {} rows", t.name, t.row_count);
            }
            if dry_run {
                println!("\n  (dry run - no changes applied)");
            }
        }
    }
    Ok(())
}

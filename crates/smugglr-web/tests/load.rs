//! The load run for @smugglr/web (NFR-SMUGGLR-010), before a release.
//!
//! It runs against the built package (packed, its engine built for
//! release, installed by an app) in headless Chromium, in a browser profile
//! kept on disk as a person's is, with the stand-in endpoint behind a front
//! that answers with gzip-compressed responses (HTTP Content-Encoding), and
//! `max_wire` at 200 MB:
//!
//! - `kills`: 500 tab syncs killed at random points, the tab closed or its
//!   renderer process killed, each reopened and synced, with rows inserted,
//!   edited and deleted on both sides, and both sides compared with a
//!   reference: 0 rows lost, 0 written twice, 0 deletes undone.
//! - `limits`: the first sync of 100 MB and the page's longest main-thread
//!   block while it lands (over 200 ms holds the release), a cleared tab's
//!   rebuild of 100 MB by first sync and by retrieve, then first syncs and
//!   rebuilds of 200-byte rows at 10k, 100k, 500k rows and doubling until
//!   the tab fails. Past `max_wire` a first sync is refused for weight, the
//!   designed path; the tab is then filled by `retrieve()` and `sync()`, and
//!   the row count where that path fails is where the tab gives out.
//!
//! Neither runs under `cargo test`; run them with
//! `cargo test -p smugglr-web --test load -- --ignored --nocapture
//! --test-threads 1`. Each prints its numbers as JSON lines starting
//! `load:`, for the Limits in packages/web/README.md.

#[path = "steps/app.rs"]
mod app;
#[allow(dead_code, reason = "shared with the scenarios, which use all of it")]
#[path = "steps/browser.rs"]
mod browser;
#[allow(dead_code, reason = "shared with the scenarios, which use all of it")]
#[path = "steps/standin.rs"]
mod standin;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::Connection;
use serde_json::{Value as Json, json};
use smugglr_core::envelope::GRAVEYARD;
use smugglr_core::hex::hex;
use smugglr_core::plan::SyncMode;
use smugglr_core::steering::Steering;
use smugglr_memory::Files;
use smugglr_multi::Settings;
use smugglr_place::LocalFile;

use browser::Browser;
use standin::StandIn;

/// The app's table, for the limits.
const ITEMS: &str = "CREATE TABLE IF NOT EXISTS items (id TEXT PRIMARY KEY, body TEXT)";

/// The app's table, and a log of every write to it that the sync ignores,
/// so a row written twice shows: for the killed syncs.
const AUDITED: &str = "CREATE TABLE IF NOT EXISTS items (id TEXT PRIMARY KEY, body TEXT); \
     CREATE TABLE IF NOT EXISTS audit (n INTEGER PRIMARY KEY, id TEXT, body TEXT); \
     CREATE TRIGGER IF NOT EXISTS items_in AFTER INSERT ON items \
       BEGIN INSERT INTO audit (id, body) VALUES (new.id, new.body); END; \
     CREATE TRIGGER IF NOT EXISTS items_up AFTER UPDATE ON items \
       BEGIN INSERT INTO audit (id, body) VALUES (new.id, new.body); END; \
     CREATE TRIGGER IF NOT EXISTS items_out AFTER DELETE ON items \
       BEGIN INSERT INTO audit (id, body) VALUES (old.id, NULL); END";

/// `max_wire` for the run.
const MAX_WIRE: u64 = 200_000_000;

/// A row's weight, the bytes of its `quote()` text: a 10-byte key and a
/// 190-byte body, 200 bytes, quoted.
const ROW_WEIGHT: u64 = 204;

/// The multi key the tab and the stashing endpoint share.
const KEY: [u8; 32] = [7; 32];

/// How long one call may take before the sync counts as no longer
/// finishing.
const PATIENCE: Duration = Duration::from_secs(30 * 60);

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// Prints one measurement.
fn report(what: &str, numbers: &Json) {
    println!("load: {}", json!({"what": what, "numbers": numbers}));
}

/// A small random source, its seed printed so a run can be repeated.
#[derive(Debug)]
struct Dice(u64);

impl Dice {
    fn roll(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// A number below `n`.
    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.roll() % u64::try_from(n.max(1)).unwrap_or(1)).unwrap_or(0)
    }
}

/// The gzip front: `packages/web/test/gzip.mjs` before the stand-in.
#[derive(Debug)]
struct Gzip {
    child: Child,
    origin: String,
}

impl Gzip {
    fn start(upstream: &str) -> Result<Self, String> {
        let web = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../packages/web");
        let mut child = Command::new("node")
            .current_dir(&web)
            .args(["test/gzip.mjs", upstream])
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|err| format!("failed to start node: {err}"))?;
        let mut origin = String::new();
        if let Some(stdout) = child.stdout.take() {
            BufReader::new(stdout)
                .read_line(&mut origin)
                .map_err(|err| err.to_string())?;
        }
        let origin = origin.trim().to_owned();
        if origin.is_empty() {
            let _ = child.kill();
            return Err("the gzip front named no origin".to_owned());
        }
        Ok(Self { child, origin })
    }
}

impl Drop for Gzip {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// One run's endpoint, its front, the app and the browser.
#[derive(Debug)]
struct Run {
    folder: PathBuf,
    app: PathBuf,
    standin: Arc<StandIn>,
    gzip: Gzip,
    browser: Browser,
    /// The app's SQL that makes its tables, at the endpoint and in the tab.
    schema: &'static str,
}

impl Run {
    fn start(name: &str, schema: &'static str) -> Result<Self, String> {
        let folder = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("load-{name}"));
        let _ = std::fs::remove_dir_all(&folder);
        std::fs::create_dir_all(&folder).map_err(|err| err.to_string())?;
        let app = folder.join("app");
        app::install(&app)?;
        let db = folder.join("endpoint.db");
        Connection::open(&db)
            .and_then(|connection| connection.execute_batch(schema))
            .map_err(|err| err.to_string())?;
        let standin = Arc::new(StandIn::start(db, Some(app.clone()))?);
        let gzip = Gzip::start(&standin.origin)?;
        let browser = Browser::launch(&gzip.origin, Some(&folder.join("profile")), PATIENCE)?;
        Ok(Self {
            folder,
            app,
            standin,
            gzip,
            browser,
            schema,
        })
    }

    fn endpoint(&self) -> Result<Connection, String> {
        let connection = Connection::open(&self.standin.db).map_err(|err| err.to_string())?;
        connection
            .busy_timeout(Duration::from_secs(60))
            .map_err(|err| err.to_string())?;
        Ok(connection)
    }

    /// Runs `sql` on the endpoint's file.
    fn at_endpoint(&self, sql: &str) -> Result<(), String> {
        self.endpoint()?
            .execute_batch(sql)
            .map_err(|err| err.to_string())
    }

    /// The endpoint's rows, by key.
    fn endpoint_rows(&self) -> Result<BTreeMap<String, String>, String> {
        let connection = self.endpoint()?;
        let mut statement = connection
            .prepare("SELECT id, body FROM items")
            .map_err(|err| err.to_string())?;
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .and_then(Iterator::collect)
            .map_err(|err| err.to_string())
    }

    /// Opens tab 0 with @smugglr/web, timing it and the page's longest
    /// main-thread block while it runs; returns the first sync's record.
    fn open(&self, max_wire: u64) -> Result<Json, String> {
        let options = json!({
            "endpoint": format!("{}/endpoint", self.gzip.origin),
            "schema": self.schema,
            "ignore": ["audit"],
            "maxWire": max_wire,
        });
        self.timed(&format!(
            "(async () => {{
               const {{ open }} = await import('@smugglr/web');
               window.tab = await open({options});
               return tab.record;
             }})()"
        ))
    }

    /// Evaluates `body`, an expression giving a promise, in tab 0, and
    /// answers `{ms, block, value}`: how long it took and the longest gap
    /// between the page's 5 ms heartbeats while it ran.
    fn timed(&self, body: &str) -> Result<Json, String> {
        self.browser.eval(
            0,
            &format!(
                "(async () => {{
                   let last = performance.now(), block = 0;
                   const beat = setInterval(() => {{
                     const at = performance.now();
                     block = Math.max(block, at - last);
                     last = at;
                   }}, 5);
                   const start = performance.now();
                   try {{
                     const value = await {body};
                     return {{ ms: performance.now() - start, block, value }};
                   }} finally {{
                     clearInterval(beat);
                   }}
                 }})()"
            ),
        )
    }

    /// Runs `sql` in tab 0's database, as the app.
    fn app(&self, sql: &str) -> Result<(), String> {
        self.browser
            .eval(0, &format!("tab.exec({})", json!(sql)))
            .map(drop)
    }

    /// The keys a sync wrote twice: the endpoint's rows into the tab, the
    /// tab's into the endpoint, by each side's audit.
    fn written_twice(&self) -> Result<Vec<String>, String> {
        let tab: Vec<(String, Option<String>)> = self
            .browser
            .eval(0, "tab.query('SELECT id, body FROM audit')")?
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|row| {
                Some((
                    row[0].as_str()?.to_owned(),
                    row[1].as_str().map(str::to_owned),
                ))
            })
            .collect();
        let connection = self.endpoint()?;
        let mut statement = connection
            .prepare("SELECT id, body FROM audit")
            .map_err(|err| err.to_string())?;
        let endpoint: Vec<(String, Option<String>)> = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .and_then(Iterator::collect)
            .map_err(|err| err.to_string())?;
        let mut keys = twice(&tab, 'e');
        keys.extend(twice(&endpoint, 't'));
        Ok(keys)
    }

    /// Tab 0's rows, by key.
    fn tab_rows(&self) -> Result<BTreeMap<String, String>, String> {
        let rows = self
            .browser
            .eval(0, "tab.query('SELECT id, body FROM items')")?;
        Ok(rows
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|row| Some((row[0].as_str()?.to_owned(), row[1].as_str()?.to_owned())))
            .collect())
    }

    fn tab_count(&self) -> Result<u64, String> {
        let count = self
            .browser
            .eval(0, "tab.query('SELECT count(*) FROM items')")?;
        count[0][0]
            .as_u64()
            .ok_or_else(|| format!("no count: {count}"))
    }

    /// Fills the endpoint with `n` rows of 200 bytes, in place of its rows.
    fn fill(&self, n: u64) -> Result<(), String> {
        self.at_endpoint(&format!(
            "DROP TABLE items; VACUUM; {ITEMS}; \
             WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < {n}) \
             INSERT INTO items SELECT printf('k%09d', i), printf('%0190d', i) FROM c"
        ))
    }

    /// Stashes the endpoint into the app's folder, where the tab fetches
    /// it as an app fetches a blob; returns its size in bytes.
    fn stash(&self) -> Result<usize, String> {
        let settings = Settings {
            store: Arc::new(Files::new(self.folder.join("stasher"))),
            key: KEY,
            node: "endpoint".to_owned(),
            targets: vec!["tab".to_owned()],
            graveyard: GRAVEYARD,
            now: now(),
            sync_mode: SyncMode::Stop,
            steering: Steering::default(),
            ignore: vec!["audit".to_owned()],
        };
        let _ = std::fs::remove_dir_all(self.folder.join("stasher"));
        let mut place = LocalFile::open(&self.standin.db).map_err(|err| err.to_string())?;
        let blob = smugglr_multi::stash(&mut place, &settings, false, |_| Ok(()))
            .map(|blob| blob.to_json())
            .map_err(|err| err.to_string())?;
        std::fs::write(self.app.join("blob.json"), &blob).map_err(|err| err.to_string())?;
        Ok(blob.len())
    }

    /// Clears the origin's data, with no tab open.
    fn clear(&self) -> Result<(), String> {
        let _ = self.browser.close(0);
        self.browser.clear()
    }

    /// Rebuilds an empty tab by retrieve, then sync: the app fetches the
    /// blob, retrieves it and syncs, timed as one.
    fn rebuild_by_retrieve(&self) -> Result<Json, String> {
        let opened = self.open(1)?;
        if opened["value"]["status"] != json!("too much") {
            return Err(format!("the tab was not left empty: {opened}"));
        }
        let multi = json!({"key": hex(&KEY), "node": "tab"});
        let timed = self.timed(&format!(
            "(async () => {{
               const blob = await (await fetch('./blob.json')).text();
               const retrieved = await tab.retrieve(blob, {multi});
               const synced = await tab.sync();
               return {{ retrieved: retrieved.status, synced: synced.status }};
             }})()"
        ))?;
        if timed["value"] == json!({"retrieved": "done", "synced": "done"}) {
            Ok(timed)
        } else {
            Err(format!("the rebuild did not finish: {timed}"))
        }
    }
}

/// The keys one side owns, and what the reference says each holds.
#[derive(Debug, Default)]
struct Side {
    rows: BTreeMap<String, String>,
    deleted: BTreeSet<String>,
    next: usize,
}

impl Side {
    /// Inserts `n` new rows of `owner`, each body marked `label`, into `sql`.
    fn insert(&mut self, owner: char, n: usize, label: &str, dice: &mut Dice, sql: &mut String) {
        for _ in 0..n {
            let id = format!("{owner}{:08}", self.next);
            self.next += 1;
            let body = format!("{owner}{label}-{:0170}", dice.roll());
            let _ = write!(sql, "INSERT INTO items VALUES ('{id}', '{body}');");
            self.rows.insert(id, body);
        }
    }

    /// Inserts 3 rows, edits 2 and deletes 1, as `owner` in `cycle`; the
    /// SQL that does it, in one transaction.
    fn mutate(&mut self, owner: char, cycle: usize, dice: &mut Dice) -> String {
        let mut sql = "BEGIN;".to_owned();
        self.insert(owner, 3, &format!("{cycle}-new"), dice, &mut sql);
        for i in 0..3 {
            let keys: Vec<&String> = self.rows.keys().collect();
            let id = keys[dice.below(keys.len())].clone();
            if i < 2 {
                let body = format!("{owner}{cycle}-edit-{:0170}", dice.roll());
                let _ = write!(sql, "UPDATE items SET body = '{body}' WHERE id = '{id}';");
                self.rows.insert(id, body);
            } else {
                let _ = write!(sql, "DELETE FROM items WHERE id = '{id}';");
                self.rows.remove(&id);
                self.deleted.insert(id);
            }
        }
        sql.push_str("COMMIT;");
        sql
    }
}

/// What one side holds, against the reference: rows lost (missing, or
/// holding an older value) and deletes undone.
fn compare(
    held: &BTreeMap<String, String>,
    sides: [&Side; 2],
    lost: &mut Vec<String>,
    undone: &mut Vec<String>,
) {
    for side in sides {
        for (id, body) in &side.rows {
            if held.get(id) != Some(body) {
                lost.push(id.clone());
            }
        }
        for id in &side.deleted {
            if held.contains_key(id) {
                undone.push(id.clone());
            }
        }
    }
}

/// The keys of `owner` that a side's audit shows written more than once
/// with one value, or deleted more than once.
fn twice(audit: &[(String, Option<String>)], owner: char) -> Vec<String> {
    let mut seen: BTreeMap<(&str, Option<&str>), usize> = BTreeMap::new();
    for (id, body) in audit {
        if id.starts_with(owner) {
            *seen.entry((id, body.as_deref())).or_default() += 1;
        }
    }
    seen.into_iter()
        .filter(|(_, n)| *n > 1)
        .map(|((id, _), _)| id.to_owned())
        .collect()
}

/// The rows each side starts with.
const START: usize = 2000;

/// The killed syncs.
const KILLS: usize = 500;

#[test]
#[ignore = "the load run, before a release"]
fn kills() -> Result<(), String> {
    let run = Run::start("kills", AUDITED)?;
    let seed = now() | 1;
    let mut dice = Dice(seed);
    let (mut tab, mut endpoint) = (Side::default(), Side::default());
    // Both sides start with rows of their own.
    let mut sql = String::new();
    endpoint.insert('e', START, "start", &mut dice, &mut sql);
    run.at_endpoint(&sql)?;
    run.open(MAX_WIRE)?;
    let mut sql = "BEGIN;".to_owned();
    tab.insert('t', START, "start", &mut dice, &mut sql);
    sql.push_str("COMMIT;");
    run.app(&sql)?;
    run.browser.eval(0, "tab.sync()")?;
    // How long a sync of one cycle's changes takes, so kills land in it.
    let mut longest = 0.0_f64;
    for cycle in 0..5 {
        run.app(&tab.mutate('t', cycle, &mut dice))?;
        run.at_endpoint(&endpoint.mutate('e', cycle, &mut dice))?;
        let timed = run.timed("tab.sync()")?;
        longest = longest.max(timed["ms"].as_f64().unwrap_or(0.0));
    }
    let window = (longest * 1.5).max(1.0);
    let (mut lost, mut undone, mut mid, mut reopened_first) = (Vec::new(), Vec::new(), 0, 0);
    for cycle in 5..5 + KILLS {
        run.app(&tab.mutate('t', cycle, &mut dice))?;
        run.at_endpoint(&endpoint.mutate('e', cycle, &mut dice))?;
        run.browser.eval(
            0,
            "(() => { window.pending = true; \
               tab.sync().finally(() => { window.pending = false; }); return null; })()",
        )?;
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss,
            reason = "a delay in whole milliseconds, below the window"
        )]
        let after = Duration::from_millis(dice.below(window as usize + 1) as u64);
        let how = if dice.below(2) == 0 { "close" } else { "crash" };
        eprintln!("load: kill {cycle}: {how} after {after:?}");
        if run.browser.kill(0, after, how)? {
            mid += 1;
        }
        let opened = run.open(MAX_WIRE)?;
        if !opened["value"].is_null() {
            reopened_first += 1;
        }
        let synced = run.browser.eval(0, "tab.sync()")?;
        if synced["status"] != json!("done") {
            return Err(format!(
                "cycle {cycle}: the sync after reopening was {synced}"
            ));
        }
        let sides = [&tab, &endpoint];
        compare(&run.tab_rows()?, sides, &mut lost, &mut undone);
        compare(&run.endpoint_rows()?, sides, &mut lost, &mut undone);
    }
    let written_twice = run.written_twice()?;
    lost.sort();
    lost.dedup();
    undone.sort();
    undone.dedup();
    report(
        "kills",
        &json!({
            "seed": seed,
            "killed": KILLS,
            "killed_mid_sync": mid,
            "kill_window_ms": window,
            "reopened_with_a_first_sync": reopened_first,
            "lost": lost.len(),
            "written_twice": written_twice.len(),
            "deletes_undone": undone.len(),
        }),
    );
    if lost.is_empty() && written_twice.is_empty() && undone.is_empty() && reopened_first == 0 {
        Ok(())
    } else {
        Err(format!(
            "lost {lost:?}, written twice {written_twice:?}, deletes undone {undone:?}, \
             {reopened_first} reopened with a first sync"
        ))
    }
}

/// The longest main-thread block that does not hold the release, in ms.
const LONGEST_BLOCK: f64 = 200.0;

/// One size of the ramp: how the first sync and the rebuild by retrieve
/// went, and whether the tab gave out.
fn ramp_step(run: &Run, rows: u64) -> Result<Json, String> {
    run.fill(rows)?;
    run.clear()?;
    let first = match run.open(MAX_WIRE) {
        Ok(timed) if timed["value"]["status"] == json!("too much") => {
            json!({"refused_for_weight": true})
        }
        Ok(timed) => match run.tab_count() {
            Ok(n) if n == rows => json!({"ms": timed["ms"], "block_ms": timed["block"]}),
            Ok(n) => json!({"failed": format!("the tab holds {n} rows: {}", timed["value"])}),
            Err(err) => json!({"failed": err}),
        },
        Err(err) => json!({"failed": err}),
    };
    run.clear()?;
    let blob = run.stash()?;
    let retrieve = match run.rebuild_by_retrieve().and_then(|timed| {
        let n = run.tab_count()?;
        if n == rows {
            Ok(timed)
        } else {
            Err(format!("the tab holds {n} rows"))
        }
    }) {
        Ok(timed) => json!({"ms": timed["ms"], "block_ms": timed["block"], "blob_bytes": blob}),
        Err(err) => json!({"failed": err, "blob_bytes": blob}),
    };
    let _ = std::fs::remove_file(run.app.join("blob.json"));
    Ok(json!({"rows": rows, "first_sync": first, "retrieve": retrieve}))
}

#[test]
#[ignore = "the load run, before a release"]
fn limits() -> Result<(), String> {
    let run = Run::start("limits", ITEMS)?;
    let mut blocks = Vec::new();
    // 100 MB: a first sync into a new tab, then a cleared tab's rebuild by
    // first sync and by retrieve.
    let rows = 100_000_000 / ROW_WEIGHT;
    run.fill(rows)?;
    let first = run.open(MAX_WIRE)?;
    if run.tab_count()? != rows {
        return Err(format!(
            "the first sync of 100 MB did not fill the tab: {first}"
        ));
    }
    run.clear()?;
    let cleared = run.open(MAX_WIRE)?;
    if run.tab_count()? != rows {
        return Err(format!("the cleared tab did not rebuild: {cleared}"));
    }
    run.clear()?;
    let blob = run.stash()?;
    let retrieved = run.rebuild_by_retrieve()?;
    if run.tab_count()? != rows {
        return Err(format!("the retrieve did not rebuild the tab: {retrieved}"));
    }
    for timed in [&first, &cleared, &retrieved] {
        blocks.push(timed["block"].as_f64().unwrap_or(f64::MAX));
    }
    report(
        "100 MB",
        &json!({
            "rows": rows,
            "first_sync_ms": first["ms"],
            "first_sync_block_ms": first["block"],
            "cleared_rebuild_by_first_sync_ms": cleared["ms"],
            "cleared_rebuild_by_first_sync_block_ms": cleared["block"],
            "cleared_rebuild_by_retrieve_ms": retrieved["ms"],
            "cleared_rebuild_by_retrieve_block_ms": retrieved["block"],
            "blob_bytes": blob,
        }),
    );
    // The ramp: 10k, 100k, 500k, then doubling, until the retrieve path
    // fails.
    let mut refused_at = None;
    let mut last_good = 0;
    let mut rows = 10_000_u64;
    let gave_out = loop {
        let step = ramp_step(&run, rows)?;
        report("ramp", &step);
        for path in ["first_sync", "retrieve"] {
            if let Some(block) = step[path]["block_ms"].as_f64() {
                blocks.push(block);
            }
        }
        if refused_at.is_none() && step["first_sync"]["refused_for_weight"] == json!(true) {
            refused_at = Some(rows);
        }
        if step["retrieve"].get("failed").is_some() {
            break step;
        }
        last_good = rows;
        rows = match rows {
            10_000 => 100_000,
            100_000 => 500_000,
            rows => rows * 2,
        };
    };
    let longest = blocks.iter().copied().fold(0.0, f64::max);
    report(
        "limits",
        &json!({
            "first_sync_refused_for_weight_at_rows": refused_at,
            "last_rows_that_rebuilt": last_good,
            "gave_out": gave_out,
            "longest_block_ms": longest,
        }),
    );
    if longest > LONGEST_BLOCK {
        return Err(format!(
            "the page's longest main-thread block was {longest} ms, over {LONGEST_BLOCK} ms: \
             a finding that holds the release"
        ));
    }
    Ok(())
}

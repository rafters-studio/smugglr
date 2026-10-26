//! Steps for @smugglr/web (FR-SMUGGLR-062): tabs in headless Chromium kept
//! in step with the stand-in endpoint.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rstest::fixture;
use rstest_bdd::Slot;
use rstest_bdd_macros::{ScenarioState, given, then, when};
use rusqlite::Connection;
use serde_json::{Value as Json, json};
use smugglr_core::envelope::GRAVEYARD;
use smugglr_core::hex::hex;
use smugglr_core::plan::SyncMode;
use smugglr_core::steering::Steering;
use smugglr_memory::Files;
use smugglr_multi::Settings;
use smugglr_place::LocalFile;

use crate::browser::Browser;
use crate::standin::StandIn;

/// The app's tables, made by the app's own SQL in `open()`.
pub(crate) const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS notes (id TEXT PRIMARY KEY, body TEXT); \
     CREATE TABLE IF NOT EXISTS tags (name TEXT PRIMARY KEY, n INTEGER)";

/// Rows in two tables: 69 bytes of `quote()` text.
const ROWS: &str = "INSERT INTO notes VALUES ('n1', 'the first note'), ('n2', 'the second note'), \
     ('n3', 'the third note'); INSERT INTO tags VALUES ('a', 1), ('b', 2)";

/// Less than the rows weigh, more than one changed row does.
const MAX_WIRE: u64 = 64;

/// The multi key the tab and the stashing device share.
const KEY: [u8; 32] = [7; 32];

/// A scenario's endpoint, its tabs and what they returned.
#[derive(Debug, Default, ScenarioState)]
pub(crate) struct Web {
    folder: Slot<PathBuf>,
    standin: Slot<Arc<StandIn>>,
    browser: Slot<Arc<Browser>>,
    /// The `maxWire` and `ignore` each `open()` names.
    max_wire: Slot<u64>,
    ignore: Slot<Vec<String>>,
    /// The last record a tab returned.
    record: Slot<Json>,
    /// What `open()` rejected with.
    refused: Slot<String>,
    /// A stash blob, as JSON text.
    blob: Slot<String>,
    /// The count of requests the endpoint had recorded at a mark.
    mark: Slot<usize>,
    /// A change hash, or a row the scenario watches.
    watched: Slot<String>,
    /// Rows read before the step under test.
    before: Slot<Json>,
    /// The folder of an app that installed the packed package, served
    /// instead of the repo's files.
    app: Slot<PathBuf>,
    /// The module the app imports @smugglr/web as; the repo's files when
    /// none.
    module: Slot<String>,
    /// The app's SQL that makes its tables; SCHEMA when none.
    pub(crate) schema: Slot<String>,
    /// The store bridge a scenario uses, as the app imports it.
    pub(crate) bridge: Slot<String>,
    /// What the bridge read, wrote and held on a server.
    pub(crate) server: Slot<Json>,
}

#[fixture]
pub(crate) fn web() -> Web {
    Web::default()
}

static NEXT: AtomicUsize = AtomicUsize::new(0);

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// The app's names for its tabs, as the `x-tab` header it sends.
fn named(tab: usize) -> &'static str {
    ["A", "B", "C"].get(tab).copied().unwrap_or("Z")
}

impl Web {
    fn schema(&self) -> String {
        self.schema.get().unwrap_or_else(|| SCHEMA.to_owned())
    }

    fn folder(&self) -> Result<PathBuf, String> {
        if let Some(folder) = self.folder.get() {
            return Ok(folder);
        }
        let folder = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "web-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&folder);
        std::fs::create_dir_all(&folder).map_err(|err| format!("{}: {err}", folder.display()))?;
        self.folder.set(folder.clone());
        Ok(folder)
    }

    /// Starts the stand-in endpoint, its file made with the app's tables
    /// and `sql`, and a browser for its origin.
    pub(crate) fn start(&self, sql: &str) -> Result<(), String> {
        let db = self.folder()?.join("endpoint.db");
        let connection = Connection::open(&db).map_err(|err| err.to_string())?;
        connection
            .execute_batch(&self.schema())
            .and_then(|()| connection.execute_batch(sql))
            .map_err(|err| err.to_string())?;
        let standin = StandIn::start(db, self.app.get())?;
        let browser = Browser::start(&standin.origin)?;
        self.standin.set(Arc::new(standin));
        self.browser.set(Arc::new(browser));
        Ok(())
    }

    fn standin(&self) -> Result<Arc<StandIn>, String> {
        self.standin
            .get()
            .ok_or_else(|| "no stand-in endpoint".to_owned())
    }

    pub(crate) fn browser(&self) -> Result<Arc<Browser>, String> {
        self.browser.get().ok_or_else(|| "no browser".to_owned())
    }

    /// Runs `sql` on the endpoint's file.
    pub(crate) fn at_endpoint(&self, sql: &str) -> Result<(), String> {
        Connection::open(&self.standin()?.db)
            .and_then(|connection| connection.execute_batch(sql))
            .map_err(|err| err.to_string())
    }

    /// Each row of `table` at the endpoint, as `quote()` text, by key.
    fn endpoint_rows(&self, table: &str) -> Result<Json, String> {
        let connection = Connection::open(&self.standin()?.db).map_err(|err| err.to_string())?;
        let mut statement = connection
            .prepare(&rows_sql(table))
            .map_err(|err| err.to_string())?;
        let width = statement.column_count();
        let rows = statement
            .query_map([], |row| {
                (0..width)
                    .map(|i| row.get::<_, String>(i))
                    .collect::<rusqlite::Result<Vec<String>>>()
            })
            .and_then(Iterator::collect::<rusqlite::Result<Vec<Vec<String>>>>)
            .map_err(|err| err.to_string())?;
        Ok(json!(rows))
    }

    /// Each row of `table` in tab `tab`'s database, as `quote()` text, by
    /// key.
    fn tab_rows(&self, tab: usize, table: &str) -> Result<Json, String> {
        self.browser()?
            .eval(tab, &format!("tab.query({})", json!(rows_sql(table))))
    }

    /// Opens tab `tab` with @smugglr/web, naming the stand-in endpoint, and
    /// returns the record of the first sync `open()` ran (null when none).
    pub(crate) fn open(&self, tab: usize) -> Result<Json, String> {
        let module = json!(
            self.module
                .get()
                .unwrap_or_else(|| "/web/index.js".to_owned())
        );
        let options = json!({
            "endpoint": format!("{}/endpoint", self.standin()?.origin),
            "headers": {"x-tab": named(tab)},
            "schema": self.schema(),
            "maxWire": self.max_wire.get(),
            "ignore": self.ignore.get().unwrap_or_default(),
        });
        let record = self.browser()?.eval(
            tab,
            &format!(
                "(async () => {{
                   const {{ open }} = await import({module});
                   window.events = [];
                   window.tab = await open({options});
                   tab.addEventListener('table-changed', (event) => events.push(event.detail));
                   return tab.record;
                 }})()"
            ),
        )?;
        self.record.set(record.clone());
        Ok(record)
    }

    /// Calls `call` on tab `tab` (`sync()`, `retrieve(...)`), and keeps the
    /// record it returns.
    fn call(&self, tab: usize, call: &str) -> Result<Json, String> {
        let record = self.browser()?.eval(tab, &format!("tab.{call}"))?;
        self.record.set(record.clone());
        Ok(record)
    }

    /// Runs `sql` in tab `tab`'s database, as the app.
    fn app(&self, tab: usize, sql: &str) -> Result<(), String> {
        self.browser()?
            .eval(tab, &format!("tab.exec({})", json!(sql)))
            .map(drop)
    }

    fn record(&self) -> Result<Json, String> {
        self.record.get().ok_or_else(|| "no record".to_owned())
    }

    /// Marks the requests the endpoint has recorded so far.
    fn mark(&self) -> Result<(), String> {
        self.mark.set(self.standin()?.log().len());
        Ok(())
    }

    /// The requests the endpoint recorded since the mark.
    fn since_mark(&self) -> Result<Vec<crate::standin::Logged>, String> {
        let mark = self.mark.get().unwrap_or(0);
        Ok(self.standin()?.log().split_off(mark))
    }

    /// The tab holds every row the endpoint holds, in both tables.
    fn in_step(&self, tab: usize) -> Result<(), String> {
        for table in ["notes", "tags"] {
            let (here, there) = (self.tab_rows(tab, table)?, self.endpoint_rows(table)?);
            if here != there {
                return Err(format!(
                    "tab {tab}'s {table} holds {here}, the endpoint {there}"
                ));
            }
        }
        Ok(())
    }

    /// Settings for a device stashing or retrieving with the shared key.
    fn settings(&self, node: &str) -> Result<Settings, String> {
        Ok(Settings {
            store: Arc::new(Files::new(self.folder()?.join(node))),
            key: KEY,
            node: node.to_owned(),
            targets: vec!["tab".to_owned()],
            graveyard: GRAVEYARD,
            now: now(),
            sync_mode: SyncMode::Stop,
            steering: Steering::default(),
            ignore: Vec::new(),
        })
    }

    /// Stashes the SQLite file at `db` as device `node`; the blob as JSON.
    fn stash(&self, db: &Path, node: &str) -> Result<String, String> {
        let mut place = LocalFile::open(db).map_err(|err| err.to_string())?;
        smugglr_multi::stash(&mut place, &self.settings(node)?, false, |_| Ok(()))
            .map(|blob| blob.to_json())
            .map_err(|err| err.to_string())
    }
}

/// Every row of `table` as its columns' `quote()` text, by key.
fn rows_sql(table: &str) -> String {
    match table {
        "notes" => "SELECT quote(id), quote(body) FROM notes ORDER BY id".to_owned(),
        "tags" => "SELECT quote(name), quote(n) FROM tags ORDER BY name".to_owned(),
        other => format!("SELECT quote(url), quote(html) FROM {other} ORDER BY url"),
    }
}

/// The multi settings a tab's app gives stash, retrieve and replay.
fn multi() -> String {
    json!({"key": hex(&KEY), "node": "tab"}).to_string()
}

/// A tab in step: the endpoint holds rows in two tables, and the tab's
/// first sync has run.
fn in_step(web: &Web) -> Result<(), String> {
    web.start(ROWS)?;
    web.open(0)?;
    web.in_step(0)?;
    web.mark()
}

#[given("a stand-in endpoint holding rows in two tables")]
fn endpoint_with_rows(web: &Web) -> Result<(), String> {
    web.start(ROWS)
}

#[when("the app opens a tab with @smugglr/web naming that endpoint")]
fn opens_tab(web: &Web) -> Result<(), String> {
    web.open(0).map(drop)
}

#[then("once open() finishes, the tab's database holds every row the endpoint holds")]
fn holds_after_open(web: &Web) -> Result<(), String> {
    web.in_step(0)
}

#[given("a tab in step with a stand-in endpoint")]
fn tab_in_step(web: &Web) -> Result<(), String> {
    in_step(web)
}

#[when("the tab's data is cleared and the app opens the tab again")]
fn cleared_and_opened(web: &Web) -> Result<(), String> {
    let browser = web.browser()?;
    browser.eval(0, "tab.close()")?;
    browser.close(0)?;
    browser.clear()?;
    let record = web.open(0)?;
    if record.is_null() {
        return Err("open() ran no first sync on a cleared tab".to_owned());
    }
    Ok(())
}

#[then("the tab's database holds every row the endpoint holds")]
fn holds_every_row(web: &Web) -> Result<(), String> {
    web.in_step(0)
}

#[given("a stand-in endpoint that drops the connection partway through a tab's first sync")]
fn drops_partway(web: &Web) -> Result<(), String> {
    web.start(ROWS)?;
    // The first sync asks the rows of each table: the second is cut off.
    web.standin()?.drop_rows(Some(2));
    match web.open(0) {
        Ok(record) => Err(format!(
            "open() finished through a dropped connection: {record}"
        )),
        Err(err) => {
            web.refused.set(err);
            Ok(())
        }
    }
}

#[when("the app opens the tab again with the connection whole")]
fn opens_again_whole(web: &Web) -> Result<(), String> {
    web.standin()?.drop_rows(None);
    web.open(0).map(drop)
}

#[then("the tab's database holds every row the endpoint holds, each once")]
fn holds_each_once(web: &Web) -> Result<(), String> {
    // Each table's rows, read by key, equal the endpoint's: none is missing
    // and none is there twice.
    web.in_step(0)
}

#[when("the app inserts a row into the tab's database and calls sync()")]
fn inserts_and_syncs(web: &Web) -> Result<(), String> {
    web.app(0, "INSERT INTO notes VALUES ('n9', 'written in the tab')")?;
    web.call(0, "sync()").map(drop)
}

#[then("the endpoint holds that row")]
fn endpoint_holds_row(web: &Web) -> Result<(), String> {
    let rows = web.endpoint_rows("notes")?;
    if rows
        .as_array()
        .is_some_and(|rows| rows.contains(&json!(["'n9'", "'written in the tab'"])))
    {
        Ok(())
    } else {
        Err(format!("the endpoint's notes are {rows}"))
    }
}

#[when("a row is changed at the endpoint and the app calls sync()")]
fn changed_at_endpoint(web: &Web) -> Result<(), String> {
    web.at_endpoint("UPDATE notes SET body = 'changed at the endpoint' WHERE id = 'n2'")?;
    web.call(0, "sync()").map(drop)
}

#[then("the tab's database holds the changed row")]
fn holds_changed_row(web: &Web) -> Result<(), String> {
    let rows = web.tab_rows(0, "notes")?;
    if rows
        .as_array()
        .is_some_and(|rows| rows.contains(&json!(["'n2'", "'changed at the endpoint'"])))
    {
        Ok(())
    } else {
        Err(format!("the tab's notes are {rows}"))
    }
}

#[when(
    "the app deletes one row in the tab, another row is deleted at the endpoint, and the app calls sync()"
)]
fn deletes_both_ways(web: &Web) -> Result<(), String> {
    web.app(0, "DELETE FROM notes WHERE id = 'n1'")?;
    web.at_endpoint("DELETE FROM notes WHERE id = 'n3'")?;
    web.call(0, "sync()").map(drop)
}

#[then("neither row is in the tab's database or at the endpoint")]
fn neither_row(web: &Web) -> Result<(), String> {
    let want = json!([["'n2'", "'the second note'"]]);
    let (here, there) = (web.tab_rows(0, "notes")?, web.endpoint_rows("notes")?);
    if here == want && there == want {
        Ok(())
    } else {
        Err(format!(
            "the tab's notes are {here}, the endpoint's {there}"
        ))
    }
}

#[given("a tab whose first sync has finished")]
fn first_sync_finished(web: &Web) -> Result<(), String> {
    in_step(web)
}

#[when("60 seconds pass with the app calling nothing")]
fn sixty_seconds(web: &Web) -> Result<(), String> {
    std::thread::sleep(Duration::from_secs(60));
    // The tab is still open and answering.
    web.browser()?.eval(0, "typeof tab.sync").map(drop)
}

#[then("the stand-in endpoint has recorded no request since the first sync")]
fn no_request_since(web: &Web) -> Result<(), String> {
    let since = web.since_mark()?;
    if since.is_empty() {
        Ok(())
    } else {
        Err(format!("{} requests since the first sync", since.len()))
    }
}

#[given("a tab opened with @smugglr/web naming a stand-in endpoint")]
fn tab_naming_endpoint(web: &Web) -> Result<(), String> {
    web.start(ROWS)
}

#[when("the tab runs a first sync and the app writes a row and calls sync()")]
fn first_sync_write_sync(web: &Web) -> Result<(), String> {
    web.open(0)?;
    web.app(0, "INSERT INTO notes VALUES ('n9', 'written in the tab')")?;
    web.call(0, "sync()").map(drop)
}

#[then(
    "every request the endpoint recorded names one of tables, hashes, rows, weights or write, and none carries SQL"
)]
fn no_sql_on_wire(web: &Web) -> Result<(), String> {
    let log = web.standin()?.log();
    let ops: Vec<String> = log
        .iter()
        .map(|logged| {
            let request: Json = serde_json::from_str(&logged.body)
                .map_err(|err| format!("a request that is not JSON ({err}): {}", logged.body))?;
            let fields = request
                .as_object()
                .ok_or_else(|| format!("a request that is not an object: {request}"))?;
            if let Some(field) = fields
                .keys()
                .find(|field| !["op", "table", "keys", "changes"].contains(&field.as_str()))
            {
                return Err(format!("a request with the field {field}: {request}"));
            }
            let words = logged.body.to_ascii_uppercase();
            if [
                "SELECT ", "INSERT ", "UPDATE ", "DELETE ", "PRAGMA ", "CREATE ",
            ]
            .iter()
            .any(|word| words.contains(word))
            {
                return Err(format!("a request carrying SQL: {}", logged.body));
            }
            let op = request["op"].as_str().unwrap_or_default();
            if ["tables", "hashes", "rows", "weights", "write"].contains(&op) {
                Ok(op.to_owned())
            } else {
                Err(format!("a request naming no place operation: {request}"))
            }
        })
        .collect::<Result<_, String>>()?;
    if ops.iter().any(|op| op == "write") {
        Ok(())
    } else {
        Err(format!("the app's row was never written: {ops:?}"))
    }
}

#[given(
    "two tabs of one origin, both in step with a stand-in endpoint, each with a row of its own written"
)]
fn two_tabs_each_a_row(web: &Web) -> Result<(), String> {
    in_step(web)?;
    web.open(1)?;
    web.app(0, "INSERT INTO notes VALUES ('na', 'written in tab A')")?;
    web.app(1, "INSERT INTO notes VALUES ('nb', 'written in tab B')")?;
    web.mark()
}

#[when("both tabs call sync() at the same moment")]
fn both_sync(web: &Web) -> Result<(), String> {
    let browser = web.browser()?;
    let a = browser.send(json!({"op": "eval", "tab": 0, "js": "tab.sync()"}))?;
    let b = browser.send(json!({"op": "eval", "tab": 1, "js": "tab.sync()"}))?;
    browser.answer(a)?;
    browser.answer(b).map(drop)
}

#[then(
    "every request of one tab's sync was recorded before the first request of the other's, and both tabs hold both rows"
)]
fn one_after_the_other(web: &Web) -> Result<(), String> {
    let tabs: Vec<String> = web
        .since_mark()?
        .into_iter()
        .map(|logged| logged.tab)
        .collect();
    let turns = tabs.windows(2).filter(|pair| pair[0] != pair[1]).count();
    if turns != 1 || !tabs.contains(&"A".to_owned()) || !tabs.contains(&"B".to_owned()) {
        return Err(format!("the requests came from tabs {tabs:?}"));
    }
    for tab in [0, 1] {
        let rows = web.tab_rows(tab, "notes")?;
        let holds = |id: &str| {
            rows.as_array()
                .is_some_and(|rows| rows.iter().any(|row| row[0] == json!(id)))
        };
        if !holds("'na'") || !holds("'nb'") {
            return Err(format!("tab {tab}'s notes are {rows}"));
        }
    }
    web.in_step(0)
}

#[given("two open tabs of one origin, in step with a stand-in endpoint")]
fn two_open_tabs(web: &Web) -> Result<(), String> {
    in_step(web)?;
    web.open(1)?;
    // Each tab listens from here: the first sync's rows were heard before.
    std::thread::sleep(Duration::from_millis(500));
    let browser = web.browser()?;
    for tab in [0, 1] {
        browser.eval(tab, "events.length = 0")?;
    }
    Ok(())
}

#[when(
    "a row is changed at the endpoint and one tab calls sync(), and the app then writes a row itself"
)]
fn changed_then_app_writes(web: &Web) -> Result<(), String> {
    web.at_endpoint("UPDATE notes SET body = 'changed at the endpoint' WHERE id = 'n2'")?;
    web.call(0, "sync()")?;
    web.app(1, "INSERT INTO notes VALUES ('n9', 'written by the app')")
}

#[then(
    "both tabs received table-changed naming that table and the changed row's key, and none for the app's own write"
)]
fn both_heard(web: &Web) -> Result<(), String> {
    let want = json!([{"table": "notes", "keys": [["'n2'"]]}]);
    let browser = web.browser()?;
    for tab in [0, 1] {
        let started = Instant::now();
        loop {
            let events = browser.eval(tab, "events")?;
            if events == want {
                break;
            }
            if started.elapsed() > Duration::from_secs(5) {
                return Err(format!("tab {tab} heard {events}"));
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    // A moment more: the app's own write fires nothing late.
    std::thread::sleep(Duration::from_millis(500));
    for tab in [0, 1] {
        let events = browser.eval(tab, "events")?;
        if events != want {
            return Err(format!("tab {tab} heard {events}"));
        }
    }
    Ok(())
}

#[when("one row is changed in the tab and differently at the endpoint, and the app calls sync()")]
fn changed_in_both(web: &Web) -> Result<(), String> {
    web.app(
        0,
        "UPDATE notes SET body = 'changed in the tab' WHERE id = 'n2'",
    )?;
    web.at_endpoint("UPDATE notes SET body = 'changed at the endpoint' WHERE id = 'n2'")?;
    web.call(0, "sync()").map(drop)
}

#[then("the record sync() returns shows that table stopped and names the row")]
fn table_stopped(web: &Web) -> Result<(), String> {
    let record = web.record()?;
    let notes = &record["tables"]["notes"];
    if notes["status"] == json!("stopped") && notes["rows"] == json!(["n2"]) {
        Ok(())
    } else {
        Err(format!("the record is {record}"))
    }
}

#[given("a stand-in endpoint holding more than max_wire")]
fn more_than_max_wire(web: &Web) -> Result<(), String> {
    web.start(ROWS)?;
    web.max_wire.set(MAX_WIRE);
    Ok(())
}

#[when("the app opens a tab naming that endpoint")]
fn opens_tab_naming(web: &Web) -> Result<(), String> {
    web.open(0).map(drop)
}

#[then("the tab's database holds no row, and the record says to fill the tab by retrieve")]
fn no_row_retrieve(web: &Web) -> Result<(), String> {
    for table in ["notes", "tags"] {
        let rows = web.tab_rows(0, table)?;
        if rows != json!([]) {
            return Err(format!("the tab's {table} holds {rows}"));
        }
    }
    let record = web.record()?;
    let why = record["why"].as_str().unwrap_or_default();
    if record["status"] == json!("too much")
        && why.contains("Fill the tab with retrieve()")
        && !why.contains("smugglr ")
    {
        Ok(())
    } else {
        Err(format!("the record is {record}"))
    }
}

#[given(
    "a stash blob made from a stand-in endpoint, and a row changed at the endpoint after the stash"
)]
fn blob_then_change(web: &Web) -> Result<(), String> {
    web.start(ROWS)?;
    let blob = web.stash(&web.standin()?.db, "endpoint")?;
    web.blob.set(blob);
    web.at_endpoint("UPDATE notes SET body = 'changed at the endpoint' WHERE id = 'n2'")?;
    // Too much for one first sync: the tab is filled by retrieve.
    web.max_wire.set(MAX_WIRE);
    Ok(())
}

#[when("the app opens an empty tab, calls retrieve() with the blob, then calls sync()")]
fn retrieve_then_sync(web: &Web) -> Result<(), String> {
    let opened = web.open(0)?;
    if opened["status"] != json!("too much") {
        return Err(format!("the tab was not left empty: {opened}"));
    }
    let blob = web.blob.get().ok_or("no blob")?;
    let retrieved = web.call(0, &format!("retrieve({}, {})", json!(blob), multi()))?;
    if retrieved["status"] != json!("done") {
        return Err(format!("the retrieve did not finish: {retrieved}"));
    }
    web.call(0, "sync()").map(drop)
}

#[then("the tab's database holds every row the endpoint holds, including the changed row")]
fn holds_including_changed(web: &Web) -> Result<(), String> {
    holds_changed_row(web)?;
    web.in_step(0)
}

#[given(
    "a tab whose retrieve() dropped an insert for a key in the graveyard, named in the record by its change hash"
)]
fn retrieve_dropped(web: &Web) -> Result<(), String> {
    // The endpoint is empty; a phone holds the app's tables and stashes.
    web.start("")?;
    web.open(0)?;
    let phone = web.folder()?.join("phone.db");
    let connection = Connection::open(&phone).map_err(|err| err.to_string())?;
    connection
        .execute_batch(SCHEMA)
        .and_then(|()| connection.execute_batch(ROWS))
        .map_err(|err| err.to_string())?;
    let whole = web.stash(&phone, "phone")?;
    connection
        .execute_batch("DELETE FROM notes WHERE id = 'n1'")
        .map_err(|err| err.to_string())?;
    let delete = web.stash(&phone, "phone")?;
    connection
        .execute_batch("INSERT INTO notes VALUES ('n1', 'made again')")
        .map_err(|err| err.to_string())?;
    let insert = web.stash(&phone, "phone")?;
    for blob in [whole, delete] {
        web.call(0, &format!("retrieve({}, {})", json!(blob), multi()))?;
    }
    let record = web.call(0, &format!("retrieve({}, {})", json!(insert), multi()))?;
    let dropped = &record["dropped"][0];
    let change = dropped["change"].as_str().unwrap_or_default();
    if dropped["key"] != json!("n1") || change.is_empty() {
        return Err(format!("the retrieve dropped no insert of n1: {record}"));
    }
    web.watched.set(change.to_owned());
    let rows = web.tab_rows(0, "notes")?;
    if rows
        .as_array()
        .is_some_and(|rows| rows.iter().any(|row| row[0] == json!("'n1'")))
    {
        return Err(format!("the dropped insert is in the tab: {rows}"));
    }
    Ok(())
}

#[when("the app calls replay() with that change hash")]
fn replays(web: &Web) -> Result<(), String> {
    let change = web.watched.get().ok_or("no change hash")?;
    // Past the change's own sunset: only a change re-sent with a fresh
    // sunset is decided rather than dropped as dead.
    let later = json!({"key": hex(&KEY), "node": "tab", "now": now() + GRAVEYARD + 3600});
    web.call(0, &format!("replay({}, {later})", json!(change)))
        .map(drop)
}

#[then(
    "the change was re-sent with a fresh sunset and decided like any change, and the record names it"
)]
fn replayed(web: &Web) -> Result<(), String> {
    let record = web.record()?;
    let change = web.watched.get().ok_or("no change hash")?;
    // Re-sent with a fresh sunset, the graveyard does not drop it again: it
    // is decided and applied, and the record names it.
    if record["replayed"] != json!(change)
        || record["status"] != json!("done")
        || record.get("dropped").is_some()
    {
        return Err(format!("the record is {record}"));
    }
    let rows = web.tab_rows(0, "notes")?;
    if rows
        .as_array()
        .is_some_and(|rows| rows.contains(&json!(["'n1'", "'made again'"])))
    {
        Ok(())
    } else {
        Err(format!("the tab's notes are {rows}"))
    }
}

#[given("a tab opened with @smugglr/web naming an endpoint that has rows the tab does not")]
fn endpoint_has_more(web: &Web) -> Result<(), String> {
    in_step(web)?;
    web.at_endpoint("INSERT INTO notes VALUES ('n7', 'only at the endpoint')")
}

#[when("the tab syncs")]
fn tab_syncs(web: &Web) -> Result<(), String> {
    web.call(0, "sync()").map(drop)
}

/// A record's field names, every status word kept, every other value
/// dropped.
fn skeleton(json: &Json) -> Json {
    match json {
        Json::Object(fields) => Json::Object(
            fields
                .iter()
                .map(|(name, value)| {
                    let value = match value {
                        Json::String(_) if name == "status" => value.clone(),
                        value => skeleton(value),
                    };
                    (name.clone(), value)
                })
                .collect(),
        ),
        Json::Array(items) => Json::Array(items.iter().map(skeleton).collect()),
        _ => Json::Null,
    }
}

/// The record of `smugglr sync --json` for the same case: the CLI's own
/// run, between a file holding a row the other does not.
fn cli_record(web: &Web) -> Result<Json, String> {
    let folder = web.folder()?.join("cli");
    std::fs::create_dir_all(&folder).map_err(|err| err.to_string())?;
    let (source, target) = (folder.join("endpoint.db"), folder.join("tab.db"));
    for (db, sql) in [(&source, ROWS), (&target, ROWS)] {
        let connection = Connection::open(db).map_err(|err| err.to_string())?;
        connection
            .execute_batch(SCHEMA)
            .and_then(|()| connection.execute_batch(sql))
            .map_err(|err| err.to_string())?;
    }
    let options = smugglr_sync::Options {
        config_dir: Some(folder.join("config")),
        ..smugglr_sync::Options::default()
    };
    let run = || {
        let mut a = LocalFile::open(&source).map_err(|err| err.to_string())?;
        let mut b = LocalFile::open(&target).map_err(|err| err.to_string())?;
        smugglr_sync::sync(&mut a, &mut b, &options).map_err(|err| err.to_string())
    };
    run()?;
    Connection::open(&source)
        .and_then(|c| c.execute_batch("INSERT INTO notes VALUES ('n7', 'only at the endpoint')"))
        .map_err(|err| err.to_string())?;
    serde_json::from_str(&run()?.json()).map_err(|err| err.to_string())
}

#[then(
    "its run record uses the same field names and status words as the record of smugglr sync --json"
)]
fn same_record(web: &Web) -> Result<(), String> {
    let (tab, cli) = (web.record()?, cli_record(web)?);
    if tab["tables"]["notes"]["to_target"] != json!(1) {
        return Err(format!("the tab's record is {tab}"));
    }
    if skeleton(&tab) == skeleton(&cli) {
        Ok(())
    } else {
        Err(format!("the tab's record is {tab}, the CLI's {cli}"))
    }
}

#[given("a tab opened with @smugglr/web that has synced with its endpoint")]
fn tab_has_synced(web: &Web) -> Result<(), String> {
    in_step(web)
}

#[when("the app lists the tables of its database")]
fn lists_tables(web: &Web) -> Result<(), String> {
    let tables = web.browser()?.eval(
        0,
        "tab.query(\"SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name\")",
    )?;
    web.before.set(tables);
    Ok(())
}

#[then(
    "it finds only the tables the app created, and the tab's memory is in the tab's own OPFS storage, outside the app's database"
)]
fn only_app_tables(web: &Web) -> Result<(), String> {
    let tables = web.before.get().ok_or("no tables listed")?;
    if tables != json!([["notes"], ["tags"]]) {
        return Err(format!("the app's database holds the tables {tables}"));
    }
    let files = web.browser()?.eval(
        0,
        "(async () => {
           const root = await navigator.storage.getDirectory();
           const names = [];
           for await (const name of root.keys()) names.push(name);
           return names.sort();
         })()",
    )?;
    let has = |name: &str| {
        files
            .as_array()
            .is_some_and(|files| files.contains(&json!(name)))
    };
    if has("app.db") && has("app.db.smugglr") {
        Ok(())
    } else {
        Err(format!("the tab's OPFS storage holds {files}"))
    }
}

#[given(
    "a tab opened with @smugglr/web with ignore holding cache_*, naming an endpoint whose cache_pages has rows the tab lacks"
)]
fn ignore_cache(web: &Web) -> Result<(), String> {
    web.start(&format!(
        "{ROWS}; CREATE TABLE cache_pages (url TEXT PRIMARY KEY, html TEXT); \
         INSERT INTO cache_pages VALUES ('/a', '<p>a</p>'), ('/b', '<p>b</p>')"
    ))?;
    web.ignore.set(vec!["cache_*".to_owned()]);
    web.open(0)?;
    web.app(
        0,
        "CREATE TABLE IF NOT EXISTS cache_pages (url TEXT PRIMARY KEY, html TEXT); \
         INSERT INTO cache_pages VALUES ('/mine', '<p>mine</p>')",
    )?;
    web.before.set(web.tab_rows(0, "cache_pages")?);
    Ok(())
}

#[when("the app calls sync()")]
fn app_syncs(web: &Web) -> Result<(), String> {
    web.call(0, "sync()").map(drop)
}

#[then("the tab's cache_pages has not changed")]
fn cache_unchanged(web: &Web) -> Result<(), String> {
    let (before, after) = (
        web.before.get().ok_or("no rows read before")?,
        web.tab_rows(0, "cache_pages")?,
    );
    let record = web.record()?;
    if before == after
        && after == json!([["'/mine'", "'<p>mine</p>'"]])
        && record["ignored"] == json!(["cache_pages"])
    {
        Ok(())
    } else {
        Err(format!(
            "the tab's cache_pages was {before}, is {after}; the record is {record}"
        ))
    }
}

/// Packs @smugglr/web with `pnpm pack`, once; the tarball's path.
fn packed() -> Result<PathBuf, String> {
    static PACKED: std::sync::OnceLock<Result<PathBuf, String>> = std::sync::OnceLock::new();
    PACKED
        .get_or_init(|| {
            let tmp = Path::new(env!("CARGO_TARGET_TMPDIR"));
            let into = tmp.join(format!("web-pack-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&into);
            std::fs::create_dir_all(&into).map_err(|err| err.to_string())?;
            let web = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../packages/web");
            pnpm(
                &web,
                &["pack", "--pack-destination", &into.display().to_string()],
                &tmp.join("smugglr-web-pack"),
            )?;
            std::fs::read_dir(&into)
                .map_err(|err| err.to_string())?
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .find(|path| path.extension().is_some_and(|ext| ext == "tgz"))
                .ok_or_else(|| "pnpm pack made no tarball".to_owned())
        })
        .clone()
}

/// Runs pnpm in `folder` with `args`, building Rust into `target`.
fn pnpm(folder: &Path, args: &[&str], target: &Path) -> Result<(), String> {
    let pnpm = if cfg!(windows) { "pnpm.cmd" } else { "pnpm" };
    let output = std::process::Command::new(pnpm)
        .current_dir(folder)
        .args(args)
        .env("CARGO_TARGET_DIR", target)
        .output()
        .map_err(|err| format!("failed to start pnpm: {err}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "pnpm {args:?} failed:\n{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

/// The app's page: it imports packages as a page with no build step does,
/// through an import map.
const APP_PAGE: &str = r#"<!doctype html>
<title>an app</title>
<script type="importmap">
{ "imports": { "@smugglr/web": "./node_modules/@smugglr/web/src/index.js" } }
</script>
"#;

#[given(
    "an app with @smugglr/web installed from the packed package, and a stand-in endpoint holding rows"
)]
fn app_installed(web: &Web) -> Result<(), String> {
    let tarball = packed()?;
    let app = web.folder()?.join("app");
    std::fs::create_dir_all(&app).map_err(|err| err.to_string())?;
    std::fs::write(
        app.join("package.json"),
        r#"{ "name": "an-app", "private": true, "type": "module" }"#,
    )
    .and_then(|()| std::fs::write(app.join("index.html"), APP_PAGE))
    .map_err(|err| err.to_string())?;
    pnpm(
        &app,
        &["add", "--ignore-workspace", &tarball.display().to_string()],
        &app.join("target"),
    )?;
    web.app.set(app);
    web.start(ROWS)
}

#[when("the app imports @smugglr/web and calls open() naming that endpoint")]
fn app_imports_and_opens(web: &Web) -> Result<(), String> {
    web.module.set("@smugglr/web".to_owned());
    web.open(0).map(drop)
}

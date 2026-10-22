//! Steps for @smugglr/nanostores and @smugglr/zustand (FR-SMUGGLR-082): a
//! store bound to a table of a tab in headless Chromium, and the same store
//! rendered on a server, in Node.

use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use rstest_bdd_macros::{given, then, when};
use serde_json::{Value as Json, json};

use crate::tab::Web;

/// The app's table the stores are bound to; it refuses a row with no title.
const TODOS: &str =
    "CREATE TABLE IF NOT EXISTS todos (id TEXT PRIMARY KEY, title TEXT NOT NULL, done INTEGER)";

/// Three rows; a scenario starts with the first `n` of them.
const ROWS: [(&str, &str, i64); 3] = [("t1", "milk", 0), ("t2", "bread", 0), ("t3", "eggs", 1)];

/// How long a step waits for the store or the tab's database to settle.
const SETTLE: Duration = Duration::from_secs(5);

/// The first `n` rows, as the store holds them, by key.
fn items(n: usize) -> Json {
    ROWS.iter()
        .take(n)
        .map(|(id, title, done)| {
            (
                (*id).to_owned(),
                json!({"id": id, "title": title, "done": done}),
            )
        })
        .collect::<serde_json::Map<_, _>>()
        .into()
}

/// A tab whose first sync filled its database with the first `n` rows of
/// todos, held at the endpoint.
fn tab_with(web: &Web, n: usize) -> Result<(), String> {
    web.schema.set(TODOS.to_owned());
    let values = ROWS
        .iter()
        .take(n)
        .map(|(id, title, done)| format!("('{id}', '{title}', {done})"))
        .collect::<Vec<_>>()
        .join(", ");
    web.start(&format!("INSERT INTO todos VALUES {values}"))?;
    web.open(0).map(drop)
}

/// Makes `bridge`'s store in tab 0, bound to todos, as `window.view`
/// (`items()`, `set(id, item)`, `remove(id)`), counting each notice the
/// store sends after its load in `window.notified` and each refusal its
/// error listener hears in `window.errors`. Returns the items the store held
/// when its load finished; when `held`, the store's tab is held back until
/// `release()` is called, and `window.ready` resolves to those items.
fn make(web: &Web, bridge: &str, held: bool) -> Result<Json, String> {
    web.bridge.set(bridge.to_owned());
    let (create, view, load) = match bridge {
        "@smugglr/nanostores" => (
            "const { map, allTasks } = await import('nanostores');
             const { bind } = await import('/nanostores/index.js');
             window.store = map({});
             bind(store, { tab: bound, table: 'todos', onError });
             window.notified = 0;
             store.listen(() => notified++);",
            "items: () => store.get(),
             set: (id, item) => store.setKey(id, item),
             remove: (id) => store.setKey(id, undefined),",
            "await allTasks();
             window.loaded = store.get();",
        ),
        "@smugglr/zustand" => (
            "const { createStore } = await import('zustand/vanilla');
             const { smugglr } = await import('/zustand/index.js');
             window.store = createStore(
               smugglr(() => ({ todos: {} }), { tab: bound, table: 'todos', collection: 'todos', onError }),
             );
             window.hydrations = 0;
             store.smugglr.onFinishHydration(() => hydrations++);
             window.early = store.smugglr.hasHydrated();
             window.notified = 0;
             store.subscribe(() => notified++);",
            "items: () => store.getState().todos,
             set: (id, item) => store.setState((state) => ({ todos: { ...state.todos, [id]: item } })),
             remove: (id) =>
               store.setState((state) => {
                 const todos = { ...state.todos };
                 delete todos[id];
                 return { todos };
               }),",
            "if (!store.smugglr.hasHydrated()) {
               await new Promise((done) => store.smugglr.onFinishHydration(done));
             }
             window.late = store.smugglr.hasHydrated();
             window.loaded = store.getState().todos;",
        ),
        other => return Err(format!("no bridge {other}")),
    };
    let bound = if held {
        "new Promise((done) => { window.release = () => done(tab); })"
    } else {
        "tab"
    };
    let wait = if held { "" } else { "return await ready;" };
    web.browser()?.eval(
        0,
        &format!(
            "(async () => {{
               window.errors = [];
               const onError = (err) => errors.push(String(err?.message ?? err));
               const bound = {bound};
               {create}
               window.view = {{ {view} }};
               window.ready = (async () => {{
                 {load}
                 notified = 0;
                 return loaded;
               }})();
               {wait}
             }})()"
        ),
    )
}

/// Waits until `js`, evaluated in tab 0, is `want`.
fn settles(web: &Web, js: &str, want: &Json) -> Result<(), String> {
    let browser = web.browser()?;
    let started = Instant::now();
    loop {
        let got = browser.eval(0, js)?;
        if &got == want {
            return Ok(());
        }
        if started.elapsed() > SETTLE {
            return Err(format!("{js} is {got}, not {want}"));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// The row of `id` in tab 0's database, as `[id, title, done]`; an empty
/// list when it is gone.
fn row(id: &str) -> String {
    format!("tab.query('SELECT id, title, done FROM todos WHERE id = ?', ['{id}'])")
}

#[given("a tab whose database holds three rows in todos")]
fn tab_three_rows(web: &Web) -> Result<(), String> {
    tab_with(web, 3)
}

#[given("a tab whose database holds rows in todos")]
fn tab_rows(web: &Web) -> Result<(), String> {
    tab_with(web, 3)
}

#[when("the app makes a store with {bridge} bound to todos and its load finishes")]
fn makes_store(web: &Web, bridge: String) -> Result<(), String> {
    make(web, &bridge, false).map(drop)
}

#[then("the store holds the three rows, each under its key")]
fn holds_three(web: &Web) -> Result<(), String> {
    settles(web, "view.items()", &items(3))
}

#[given("a store made with {bridge} bound to todos, loaded")]
fn store_loaded(web: &Web, bridge: String) -> Result<(), String> {
    tab_with(web, 3)?;
    make(web, &bridge, false).map(drop)
}

/// `bridge`'s store, loaded from a tab holding the first `n` rows; fails
/// unless it loaded exactly those.
fn loaded_with(web: &Web, bridge: &str, n: usize) -> Result<(), String> {
    tab_with(web, n)?;
    let loaded = make(web, bridge, false)?;
    if loaded == items(n) {
        Ok(())
    } else {
        Err(format!("the store loaded {loaded}"))
    }
}

#[given("a store made with {bridge} bound to todos, loaded, holding a row")]
fn store_one_row(web: &Web, bridge: String) -> Result<(), String> {
    loaded_with(web, &bridge, 1)
}

#[given("a store made with {bridge} bound to todos, loaded, holding two rows")]
fn store_two_rows(web: &Web, bridge: String) -> Result<(), String> {
    loaded_with(web, &bridge, 2)
}

#[when("the app sets one key in the store")]
fn sets_key(web: &Web) -> Result<(), String> {
    web.browser()?
        .eval(
            0,
            "view.set('t9', { id: 't9', title: 'set in the store', done: 0 })",
        )
        .map(drop)
}

#[then("the tab's database holds that row")]
fn holds_set_row(web: &Web) -> Result<(), String> {
    settles(web, &row("t9"), &json!([["t9", "set in the store", 0]]))
}

#[when("the app removes that key from the store")]
fn removes_key(web: &Web) -> Result<(), String> {
    web.browser()?.eval(0, "view.remove('t1')").map(drop)
}

#[then("the row is gone from the tab's database")]
fn row_gone(web: &Web) -> Result<(), String> {
    settles(web, &row("t1"), &json!([]))
}

#[when("a sync changes one of the rows in the tab's database")]
fn sync_changes_row(web: &Web) -> Result<(), String> {
    let browser = web.browser()?;
    browser.eval(0, "window.other = view.items().t2")?;
    web.at_endpoint("UPDATE todos SET title = 'changed by a sync' WHERE id = 't1'")?;
    browser.eval(0, "tab.sync()").map(drop)
}

#[then("the store holds the changed row, and the other item is the same object as before")]
fn changed_row_only(web: &Web) -> Result<(), String> {
    settles(web, "view.items().t1.title", &json!("changed by a sync"))?;
    settles(web, "view.items().t2 === other", &json!(true))
}

#[then("the store is notified once, for the app's own set")]
fn notified_once(web: &Web) -> Result<(), String> {
    // The row is written, and a moment more passes: a reload would come now.
    settles(web, &row("t9"), &json!([["t9", "set in the store", 0]]))?;
    std::thread::sleep(Duration::from_millis(500));
    let notified = web.browser()?.eval(0, "notified")?;
    if notified == json!(1) {
        Ok(())
    } else {
        Err(format!("the store was notified {notified} times"))
    }
}

#[given("a store made with {bridge} bound to todos, rendered on a server")]
fn store_on_server(web: &Web, bridge: String) {
    web.bridge.set(bridge);
}

/// Makes the store in Node, with no window, bound to a tab that records
/// every use; reads it once its tasks and a moment have passed.
#[when("the app reads the store")]
fn reads_store(web: &Web) -> Result<(), String> {
    let bridge = web.bridge.get().ok_or("no bridge")?;
    let (folder, js) = match bridge.as_str() {
        "@smugglr/nanostores" => (
            "nanostores",
            "import { map, allTasks } from 'nanostores';
             import { bind } from './src/index.js';
             const store = map({ initial: { id: 'initial' } });
             bind(store, { tab, table: 'todos' });
             store.listen(() => {});
             await allTasks();
             const state = () => store.get();",
        ),
        "@smugglr/zustand" => (
            "zustand",
            "import { createStore } from 'zustand/vanilla';
             import { smugglr } from './src/index.js';
             const store = createStore(
               smugglr(() => ({ todos: { initial: { id: 'initial' } } }), { tab, table: 'todos', collection: 'todos' }),
             );
             const state = () => store.getState().todos;",
        ),
        other => return Err(format!("no bridge {other}")),
    };
    let script = format!(
        "const used = [];
         const tab = new Proxy({{}}, {{ get: (_, name) => (...args) => {{ used.push(String(name)); return Promise.resolve([]); }} }});
         {js}
         await new Promise((done) => setTimeout(done, 200));
         console.log(JSON.stringify({{ used, state: state() }}));"
    );
    let output = Command::new("node")
        .current_dir(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../packages")
                .join(folder),
        )
        .args(["--input-type=module", "-e", &script])
        .output()
        .map_err(|err| format!("failed to start node: {err}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned());
    }
    let seen = serde_json::from_slice(&output.stdout).map_err(|err| err.to_string())?;
    web.server.set(seen);
    Ok(())
}

#[then("the bridge has read and written nothing, and the store holds its initial state")]
fn nothing_on_server(web: &Web) -> Result<(), String> {
    let seen = web.server.get().ok_or("the store was not read")?;
    let want = json!({"used": [], "state": {"initial": {"id": "initial"}}});
    if seen == want {
        Ok(())
    } else {
        Err(format!("on the server: {seen}"))
    }
}

#[when("the app makes a store with @smugglr/zustand bound to todos")]
fn makes_zustand(web: &Web) -> Result<(), String> {
    make(web, "@smugglr/zustand", false).map(drop)
}

#[then(
    "hasHydrated() is false before the load finishes and true after, and an onFinishHydration listener was called once"
)]
fn hydrated(web: &Web) -> Result<(), String> {
    // A moment more: the listener is not called again.
    std::thread::sleep(Duration::from_millis(500));
    let seen = web
        .browser()?
        .eval(0, "({ early, late, hydrations, loaded })")?;
    let want = json!({"early": false, "late": true, "hydrations": 1, "loaded": items(3)});
    if seen == want {
        Ok(())
    } else {
        Err(format!("the store said {seen}"))
    }
}

#[when("the app mounts a store made with @smugglr/nanostores bound to todos and awaits allTasks()")]
fn mounts_nanostores(web: &Web) -> Result<(), String> {
    make(web, "@smugglr/nanostores", false).map(drop)
}

#[then("the store holds every row when allTasks() resolves")]
fn held_at_all_tasks(web: &Web) -> Result<(), String> {
    let loaded = web.browser()?.eval(0, "loaded")?;
    if loaded == items(3) {
        Ok(())
    } else {
        Err(format!("when allTasks() resolved the store held {loaded}"))
    }
}

#[given(
    "a store made with {bridge} bound to todos, loaded, whose table refuses a row with no title"
)]
fn store_refusing(web: &Web, bridge: String) -> Result<(), String> {
    // todos' title is NOT NULL.
    loaded_with(web, &bridge, 3)
}

#[when("the app sets a key in the store to a value with no title")]
fn sets_no_title(web: &Web) -> Result<(), String> {
    web.browser()?
        .eval(0, "view.set('t1', { id: 't1', done: 1 })")
        .map(drop)
}

#[then(
    "the store holds what the table holds for that key, and the bridge's error listener received the refusal"
)]
fn put_back_and_reported(web: &Web) -> Result<(), String> {
    settles(web, "errors.length", &json!(1))?;
    settles(web, "view.items().t1", &items(1)["t1"])?;
    settles(web, &row("t1"), &json!([["t1", "milk", 0]]))?;
    let errors = web.browser()?.eval(0, "errors")?;
    if errors.to_string().contains("NOT NULL") {
        Ok(())
    } else {
        Err(format!("the error listener heard {errors}"))
    }
}

#[given(
    "a tab whose database holds rows in todos, and a store made with {bridge} bound to todos whose load has not finished"
)]
fn store_not_loaded(web: &Web, bridge: String) -> Result<(), String> {
    tab_with(web, 3)?;
    make(web, &bridge, true).map(drop)
}

#[when("the app sets a new key in the store before the load finishes")]
fn sets_before_load(web: &Web) -> Result<(), String> {
    web.browser()?
        .eval(
            0,
            "view.set('t9', { id: 't9', title: 'set in the store', done: 0 }); release()",
        )
        .map(drop)
}

#[then("once the load finishes, the store and the tab's database both hold that key")]
fn kept_after_load(web: &Web) -> Result<(), String> {
    let t9 = json!({"id": "t9", "title": "set in the store", "done": 0});
    let loaded = web.browser()?.eval(0, "ready")?;
    let mut want = items(3);
    want["t9"] = t9.clone();
    if loaded != want {
        return Err(format!("when its load finished the store held {loaded}"));
    }
    settles(web, "view.items().t9", &t9)?;
    settles(web, &row("t9"), &json!([["t9", "set in the store", 0]]))
}

#[when(
    "a sync changes one row and the app changes the other key in the store before the reload finishes"
)]
fn sync_and_app_change(web: &Web) -> Result<(), String> {
    let browser = web.browser()?;
    // The app's change lands as table-changed is heard, while the bridge's
    // reload is still reading the changed row.
    browser.eval(
        0,
        "tab.addEventListener('table-changed', () => view.set('t2', { id: 't2', title: 'changed in the store', done: 1 }), { once: true })",
    )?;
    web.at_endpoint("UPDATE todos SET title = 'changed by a sync' WHERE id = 't1'")?;
    browser.eval(0, "tab.sync()").map(drop)
}

#[then("the store holds the synced row and the app's change, and the tab's database holds both")]
fn both_kept(web: &Web) -> Result<(), String> {
    settles(web, "view.items().t1.title", &json!("changed by a sync"))?;
    // A moment more: a late reload would overwrite the app's change by now.
    std::thread::sleep(Duration::from_millis(500));
    settles(
        web,
        "view.items().t2",
        &json!({"id": "t2", "title": "changed in the store", "done": 1}),
    )?;
    settles(web, &row("t1"), &json!([["t1", "changed by a sync", 0]]))?;
    settles(web, &row("t2"), &json!([["t2", "changed in the store", 1]]))
}

#[when(
    "a sync changes that row and the app changes the same key in the store before the reload finishes"
)]
fn sync_and_app_change_same(web: &Web) -> Result<(), String> {
    let browser = web.browser()?;
    // The app's change lands as table-changed is heard, while the bridge's
    // reload is still reading that same row.
    browser.eval(
        0,
        "tab.addEventListener('table-changed', () => view.set('t1', { id: 't1', title: 'changed in the store', done: 1 }), { once: true })",
    )?;
    web.at_endpoint("UPDATE todos SET title = 'changed by a sync' WHERE id = 't1'")?;
    browser.eval(0, "tab.sync()").map(drop)
}

#[then("the store and the tab's database both hold the app's change")]
fn app_change_kept(web: &Web) -> Result<(), String> {
    settles(web, &row("t1"), &json!([["t1", "changed in the store", 1]]))?;
    // A moment more: a late reload would put the sync's older read back now.
    std::thread::sleep(Duration::from_millis(500));
    settles(
        web,
        "view.items().t1",
        &json!({"id": "t1", "title": "changed in the store", "done": 1}),
    )?;
    settles(web, &row("t1"), &json!([["t1", "changed in the store", 1]]))
}

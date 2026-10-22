//! What the tab's worker gives the engine, as `globalThis.smugglrHost`: its
//! SQLite connections and the HTTP of the endpoint. Each call is
//! synchronous, as a place is; the worker throws a failure as text.

use wasm_bindgen::prelude::{JsValue, wasm_bindgen};

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = smugglrHost, js_name = fetch, catch)]
    fn host_fetch(request: &str) -> Result<String, JsValue>;
    #[wasm_bindgen(js_namespace = smugglrHost, js_name = sql, catch)]
    fn host_sql(request: &str) -> Result<String, JsValue>;
    #[wasm_bindgen(js_namespace = smugglrHost, js_name = memRead, catch)]
    fn host_mem_read(path: &str) -> Result<Option<String>, JsValue>;
    #[wasm_bindgen(js_namespace = smugglrHost, js_name = memReplace, catch)]
    fn host_mem_replace(path: &str, text: &str) -> Result<(), JsValue>;
    #[wasm_bindgen(js_namespace = smugglrHost, js_name = memAppend, catch)]
    fn host_mem_append(path: &str, text: &str) -> Result<(), JsValue>;
    #[wasm_bindgen(js_namespace = smugglrHost, js_name = memRemove, catch)]
    fn host_mem_remove(path: &str) -> Result<(), JsValue>;
}

fn words(err: &JsValue) -> String {
    err.as_string().unwrap_or_else(|| format!("{err:?}"))
}

/// Sends one place operation to the endpoint and returns its answer.
pub(crate) fn fetch(request: &str) -> Result<String, String> {
    host_fetch(request).map_err(|err| words(&err))
}

/// Runs one HTTP-SQL request on the tab's database and returns its answer.
pub(crate) fn sql(request: &str) -> Result<String, String> {
    host_sql(request).map_err(|err| words(&err))
}

/// The text of a file of the tab's memory; `None` when there is none.
pub(crate) fn mem_read(path: &str) -> Result<Option<String>, String> {
    host_mem_read(path).map_err(|err| words(&err))
}

/// Replaces a file of the tab's memory, in one transaction.
pub(crate) fn mem_replace(path: &str, text: &str) -> Result<(), String> {
    host_mem_replace(path, text).map_err(|err| words(&err))
}

/// Appends to a file of the tab's memory.
pub(crate) fn mem_append(path: &str, text: &str) -> Result<(), String> {
    host_mem_append(path, text).map_err(|err| words(&err))
}

/// Removes a file of the tab's memory.
pub(crate) fn mem_remove(path: &str) -> Result<(), String> {
    host_mem_remove(path).map_err(|err| words(&err))
}

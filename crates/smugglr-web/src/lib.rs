//! The smugglr engine in the browser (FR-SMUGGLR-058).
//!
//! Compiled to wasm32 with wasm-bindgen, so a tab runs the same engine as
//! the command line. The browser's SQLite registers [`row_hash`] as its
//! `smugglr_row_hash` function, so each row is hashed where it lives, from
//! its columns' `quote()` text, exactly as a local file hashes it.

use wasm_bindgen::prelude::wasm_bindgen;

/// Hashes one row, given SQLite's `quote()` text of each column in order:
/// the engine's one row hash, as its 16 bytes.
#[wasm_bindgen(js_name = rowHash)]
#[must_use]
#[expect(
    clippy::needless_pass_by_value,
    reason = "wasm-bindgen hands the columns over from JS as an owned Vec"
)]
pub fn row_hash(columns: Vec<String>) -> Vec<u8> {
    let columns: Vec<&str> = columns.iter().map(String::as_str).collect();
    smugglr_core::row_hash::row_hash(&columns).to_vec()
}

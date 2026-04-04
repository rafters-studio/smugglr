//! Generic HTTP SQL adapter plugin for smuggler.
//!
//! Works with any HTTP endpoint that accepts SQL queries via POST and
//! returns JSON results. Built-in profiles for Turso, rqlite, and
//! generic endpoints. Custom profiles via config.

mod adapter;
mod profile;

#[tokio::main]
async fn main() {
    smuggler_plugin_sdk::run(adapter::HttpSqlAdapter::new()).await;
}

//! smugglr keeps SQLite in step with SQLite.
//!
//! Inside a program, [`sync`] runs one sync between two places, the same
//! sync `smugglr sync` runs. [`Config`] reads the person's config, as the
//! CLI does.

pub mod config;
mod shape;
mod sync;

pub use config::Config;
pub use sync::{Error, Options, sync};

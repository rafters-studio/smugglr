//! smugglr keeps SQLite in step with SQLite.
//!
//! Inside a program, [`sync`] runs one sync between two places, the same
//! sync `smugglr sync` runs.

mod sync;

pub use sync::{Error, Options, sync};

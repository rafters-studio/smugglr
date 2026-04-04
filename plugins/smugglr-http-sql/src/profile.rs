//! Re-export profiles from smugglr-core.
//!
//! Profile definitions live in smugglr-core so both the http-sql plugin
//! (reqwest) and the WASM adapter (fetch) share the same code.

pub use smugglr_core::profile::*;

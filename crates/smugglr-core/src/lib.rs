//! The smugglr sync engine.
//!
//! Greenfield rebuild. This crate is the portable core: the content-hashing
//! and diff engine. It is pure: no I/O, no clock (now is passed in), and it
//! compiles to WASM.
//!
//! Code arrives as the spec defines it.

pub mod change;
pub mod row_hash;

//! Recovery: surgical operation log + `--paranoid` snapshot (pre-stub). Body
//! lands in #289 -- a write-ahead, per-step, chain-hashed log in its own sidecar
//! DB, plus a `VACUUM INTO` coarse fallback. Empty until then.

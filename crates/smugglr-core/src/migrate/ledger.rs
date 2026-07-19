//! The migration ledger: `_smugglr_migrations`, migrate's own tamper-evident,
//! concurrency-safe, observable record of which migrations have applied.
//!
//! This is the most load-bearing artifact in the design (`docs/plans/migration.md`
//! decisions 6 and 7). It is **migrate's own table** and never wraps `wrangler`'s
//! tracker. It carries five invariants:
//!
//! - **`UNIQUE(version)` election.** Exactly one node wins the right to apply a
//!   given version. There is **no explicit transaction** -- the race is resolved
//!   by the unique constraint on `INSERT` (detected via SQLite extended code
//!   `2067`, [`SQLITE_CONSTRAINT_UNIQUE`]), so the whole flow is portable to D1,
//!   which has no interactive transactions. Everything runs on a single
//!   [`&Connection`](rusqlite::Connection).
//! - **Success-gated skip, never row-existence.** A row is skipped only when its
//!   `status = 'success'`. A crashed-mid-apply *pending* row must **re-drive**,
//!   not be skipped -- otherwise a node marches to `vN+1` against a half-applied
//!   `vN`, which is fabric-wide corruption (design decision 6).
//! - **Leased, idempotently re-driven pending.** A pending row carries a lease;
//!   an expired lease is reclaimable by any node and re-driven (apply is
//!   idempotent, so re-driving is safe). A `failed` row is likewise reclaimable.
//! - **Per-version chain-hash tamper-evidence.** Each entry hashes the prior
//!   entry (`SHA-256`, reusing the `sha2` primitive). An out-of-band
//!   `UPDATE`/`DELETE` breaks the chain and is detected on the next run
//!   ([`Ledger::verify_chain`] -> [`SyncError::LedgerTampered`]).
//! - **Observable current version.** [`Ledger::current_version`] is stable and
//!   external (`MAX(version) WHERE status = 'success'`), so a partitioned laggard
//!   is detectable (design "Migrate x sync").
//!
//! Two forward-compat nullable columns are provisioned **up front**
//! (`preimage_ref`, `schema_projection`) because the ledger tracks migrations and
//! cannot be cleanly `ALTER`ed later. This issue only *provisions* them; #274 and
//! #290 write and read them. Both sit **outside** the chain-hash input, so writing
//! them later does not break the chain.

#![cfg(feature = "native")]

use crate::error::{Result, SyncError};
use rusqlite::{Connection, OptionalExtension};
use sha2::{Digest, Sha256};

/// The namespaced ledger table. App introspection/reset ignores it, and it is
/// appended to `config::default_exclude_tables` so `validate` cannot see it.
pub const LEDGER_TABLE: &str = "_smugglr_migrations";

/// SQLite extended result code for a `UNIQUE` constraint violation.
///
/// The ledger elects an apply-winner by racing an `INSERT` against `UNIQUE(version)`
/// with no transaction; the race-loser's `INSERT` fails with exactly this code.
/// This detection is correct **only** because `version` is a plain `UNIQUE` column
/// with an implicit rowid -- were it declared `INTEGER PRIMARY KEY`, the violation
/// would surface as `1555` (`SQLITE_CONSTRAINT_PRIMARYKEY`) and the race detection
/// would silently never fire. Keep the schema `UNIQUE(version)` with no declared PK.
const SQLITE_CONSTRAINT_UNIQUE: i32 = 2067;

/// Default lease lifetime for a pending row, in seconds.
///
/// A node that claims a version holds it for this long; if it crashes mid-apply,
/// another node can reclaim and re-drive once the lease expires.
pub const DEFAULT_LEASE_SECS: i64 = 300;

/// The genesis predecessor hash for the first ledger entry.
const GENESIS_PREV_HASH: &str = "";

/// The status of a ledger entry -- a closed enum, never a bare string.
///
/// Stored in the `status` TEXT column as its [`Self::as_str`] rendering; the Rust
/// side always works in terms of the enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationStatus {
    /// The apply is in progress (or crashed mid-apply). Leased; reclaimable when
    /// the lease expires. **Never** skipped by the gate.
    Pending,
    /// The apply completed. This is the only status the skip-gate keys on.
    Success,
    /// The apply failed. Reclaimable and re-driven (apply is idempotent).
    Failed,
}

impl MigrationStatus {
    /// The stored TEXT rendering.
    pub fn as_str(&self) -> &'static str {
        match self {
            MigrationStatus::Pending => "pending",
            MigrationStatus::Success => "success",
            MigrationStatus::Failed => "failed",
        }
    }

    /// Parse a stored TEXT rendering back into the enum.
    ///
    /// An unrecognized value is itself a tamper signal (someone wrote an
    /// out-of-band status), so it maps to [`SyncError::LedgerTampered`].
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "pending" => Ok(MigrationStatus::Pending),
            "success" => Ok(MigrationStatus::Success),
            "failed" => Ok(MigrationStatus::Failed),
            other => Err(SyncError::LedgerTampered(format!(
                "unrecognized ledger status '{other}'"
            ))),
        }
    }
}

/// The outcome of an election attempt ([`Ledger::try_elect`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Election {
    /// This node won the right to apply the version (a fresh claim, or a reclaim
    /// of an expired-pending / failed row). The caller drives the migration, then
    /// calls [`Ledger::mark_success`] or [`Ledger::mark_failed`].
    Won,
    /// The version is already applied (`status = 'success'`). Skip it.
    AlreadyApplied,
    /// Another node holds a live lease on a pending row. Back off and retry later.
    HeldByOther,
}

/// A single ledger row.
///
/// The forward-compat columns [`preimage_ref`](Self::preimage_ref) and
/// [`schema_projection`](Self::schema_projection) are read here so #274 / #290 can
/// consume them, but this issue never writes them. They are outside the chain-hash
/// input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerEntry {
    /// The authored, monotonic migration version.
    pub version: u64,
    /// The SHA-256 content identity of the applied manifest.
    pub checksum: String,
    /// The apply status.
    pub status: MigrationStatus,
    /// Unix timestamp (seconds) of the last claim / completion.
    pub applied_at: i64,
    /// Unix timestamp (seconds) at which a pending lease expires; `None` once the
    /// row is no longer pending.
    pub lease_expires_at: Option<i64>,
    /// The chain-hash of the predecessor entry (genesis is the empty string).
    pub prev_hash: String,
    /// This entry's chain-hash: `SHA-256(version || checksum || prev_hash)`.
    pub chain_hash: String,
    /// Forward-compat: content-addressed pre-image reference (#274 writes it).
    pub preimage_ref: Option<String>,
    /// Forward-compat: the full canonical schema projection JSON (#290 writes it).
    pub schema_projection: Option<String>,
}

/// The migration ledger. A thin, stateless namespace over a borrowed connection;
/// every method takes `&Connection` and runs without an explicit transaction so
/// the whole surface is D1-portable.
pub struct Ledger;

impl Ledger {
    /// Create the ledger table if it does not exist.
    ///
    /// `version` is a plain `UNIQUE` column (not a declared PK) so the election
    /// race surfaces as [`SQLITE_CONSTRAINT_UNIQUE`]. The two forward-compat
    /// columns are provisioned here up front and left nullable.
    pub fn ensure_schema(conn: &Connection) -> Result<()> {
        conn.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS \"{LEDGER_TABLE}\" (
                 version           INTEGER NOT NULL,
                 checksum          TEXT    NOT NULL,
                 status            TEXT    NOT NULL,
                 applied_at        INTEGER NOT NULL,
                 lease_expires_at  INTEGER,
                 prev_hash         TEXT    NOT NULL,
                 chain_hash        TEXT    NOT NULL,
                 preimage_ref      TEXT,
                 schema_projection TEXT,
                 UNIQUE(version)
             );"
        ))?;
        Ok(())
    }

    /// The highest **successfully applied** version, or `None` if none.
    ///
    /// Keyed on `status = 'success'` so a crashed-pending (or failed) row never
    /// advances the observable version. Stable across #278/#291.
    pub fn current_version(conn: &Connection) -> Result<Option<u64>> {
        let v: Option<i64> = conn.query_row(
            &format!("SELECT MAX(version) FROM \"{LEDGER_TABLE}\" WHERE status = 'success'"),
            [],
            |row| row.get(0),
        )?;
        Ok(v.map(|v| v as u64))
    }

    /// Attempt to elect this node as the applier of `version`.
    ///
    /// Transaction-free. The decision:
    /// - no row -> `INSERT` a pending, leased row; the `UNIQUE(version)` winner
    ///   returns [`Election::Won`], a race-loser re-reads and re-decides.
    /// - `success` -> [`Election::AlreadyApplied`] (the skip-gate).
    /// - live-leased `pending` -> [`Election::HeldByOther`].
    /// - expired `pending` or `failed` -> reclaim via a compare-and-set `UPDATE`
    ///   (renews the lease); the CAS winner returns [`Election::Won`], a loser
    ///   re-reads.
    ///
    /// `lease_secs` sets the reclaim window for the pending row this node writes.
    pub fn try_elect(
        conn: &Connection,
        version: u64,
        checksum: &str,
        lease_secs: i64,
    ) -> Result<Election> {
        // Bounded so a pathological concurrent-writer storm cannot spin forever;
        // each iteration re-reads authoritative state, so a small bound suffices.
        const MAX_ATTEMPTS: usize = 8;
        for _ in 0..MAX_ATTEMPTS {
            match Self::entry(conn, version)? {
                None => match Self::insert_pending(conn, version, checksum, lease_secs) {
                    Ok(true) => return Ok(Election::Won),
                    Ok(false) => {
                        // The tail moved between our read and the guarded insert
                        // (a concurrent distinct-version append). Nothing was
                        // written; re-read and re-derive against the new tail.
                        continue;
                    }
                    Err(SyncError::LocalDb(e)) if is_unique_violation(&e) => {
                        // Race lost: another node inserted first. Re-read and
                        // re-decide (it is now pending/success).
                        continue;
                    }
                    Err(e) => return Err(e),
                },
                Some(entry) => match entry.status {
                    MigrationStatus::Success => return Ok(Election::AlreadyApplied),
                    MigrationStatus::Pending => {
                        if !lease_expired(entry.lease_expires_at, now_unix()) {
                            return Ok(Election::HeldByOther);
                        }
                        // Expired pending -> a crashed applier. Reclaim it.
                        if Self::reclaim_pending(conn, version, lease_secs)? {
                            return Ok(Election::Won);
                        }
                        // Someone reclaimed between our read and CAS; re-read.
                        continue;
                    }
                    MigrationStatus::Failed => {
                        // A failed apply is reclaimable and re-driven -- the gate
                        // skips only on success, never on row-existence.
                        if Self::reclaim_failed(conn, version, lease_secs)? {
                            return Ok(Election::Won);
                        }
                        continue;
                    }
                },
            }
        }
        // Exhausted attempts against constant churn: treat as held so the caller
        // backs off rather than busy-looping.
        Ok(Election::HeldByOther)
    }

    /// Mark a won version as successfully applied. Idempotent.
    ///
    /// Clears the lease and stamps the completion time. Leaves `version` and
    /// `checksum` untouched, so the chain-hash is unaffected.
    pub fn mark_success(conn: &Connection, version: u64) -> Result<()> {
        conn.execute(
            &format!(
                "UPDATE \"{LEDGER_TABLE}\"
                 SET status = 'success', lease_expires_at = NULL, applied_at = ?1
                 WHERE version = ?2"
            ),
            rusqlite::params![now_unix(), version as i64],
        )?;
        Ok(())
    }

    /// Mark a won version as failed. Idempotent.
    ///
    /// Clears the lease; the row stays reclaimable (a later [`Self::try_elect`]
    /// re-drives it). Leaves `version` and `checksum` untouched.
    pub fn mark_failed(conn: &Connection, version: u64) -> Result<()> {
        conn.execute(
            &format!(
                "UPDATE \"{LEDGER_TABLE}\"
                 SET status = 'failed', lease_expires_at = NULL
                 WHERE version = ?1"
            ),
            rusqlite::params![version as i64],
        )?;
        Ok(())
    }

    /// Read a single entry by version.
    pub fn entry(conn: &Connection, version: u64) -> Result<Option<LedgerEntry>> {
        let mut stmt = conn.prepare(&format!(
            "SELECT version, checksum, status, applied_at, lease_expires_at, \
                    prev_hash, chain_hash, preimage_ref, schema_projection
             FROM \"{LEDGER_TABLE}\" WHERE version = ?1"
        ))?;
        let mut rows = stmt.query(rusqlite::params![version as i64])?;
        match rows.next()? {
            Some(row) => Ok(Some(row_to_entry(row)?)),
            None => Ok(None),
        }
    }

    /// Read all entries in ascending version order.
    pub fn entries(conn: &Connection) -> Result<Vec<LedgerEntry>> {
        let mut stmt = conn.prepare(&format!(
            "SELECT version, checksum, status, applied_at, lease_expires_at, \
                    prev_hash, chain_hash, preimage_ref, schema_projection
             FROM \"{LEDGER_TABLE}\" ORDER BY version ASC"
        ))?;
        let mut out = Vec::new();
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            out.push(row_to_entry(row)?);
        }
        Ok(out)
    }

    /// Verify the chain-hash of every entry, in version order.
    ///
    /// Recomputes each entry's chain-hash from its `version`, `checksum`, and
    /// stored `prev_hash`, and checks that `prev_hash` links to the predecessor's
    /// chain-hash. Any out-of-band `UPDATE` (altered checksum/version, forged
    /// chain-hash) or middle `DELETE` breaks the chain and returns
    /// [`SyncError::LedgerTampered`].
    ///
    /// Deleting the *tail* is not tamper: those versions simply drop below
    /// `current_version` and re-drive idempotently, re-linking the chain
    /// identically -- so the tail case is not (and need not be) detected here.
    pub fn verify_chain(conn: &Connection) -> Result<()> {
        let mut prev = GENESIS_PREV_HASH.to_string();
        for entry in Self::entries(conn)? {
            if entry.prev_hash != prev {
                return Err(SyncError::LedgerTampered(format!(
                    "broken chain link at version {}: prev_hash does not match the \
                     predecessor entry (a row was deleted, reordered, or edited)",
                    entry.version
                )));
            }
            let expected = chain_hash(entry.version, &entry.checksum, &entry.prev_hash);
            if expected != entry.chain_hash {
                return Err(SyncError::LedgerTampered(format!(
                    "chain-hash mismatch at version {}: entry was modified out of band",
                    entry.version
                )));
            }
            prev = entry.chain_hash;
        }
        Ok(())
    }

    /// Insert a fresh pending, leased row, chaining onto the current tail.
    ///
    /// Returns `Ok(true)` when the row was inserted, `Ok(false)` when the tail
    /// moved under us between the read and the write (a concurrent *distinct*
    /// version won the append) so [`Self::try_elect`] must re-read and retry. A
    /// `UNIQUE(version)` violation still surfaces as `Err` so the same-version
    /// race-loser path in [`Self::try_elect`] is unchanged.
    ///
    /// MED 2 -- atomic, fork-safe chaining. The tail is read here to compute the
    /// chain-hash, then [`Self::insert_pending_chained`] re-validates that read
    /// against `MAX(version)`'s live chain-hash *inside the single INSERT* (a
    /// compare-and-set). `UNIQUE(version)` only serializes the *same* version, so
    /// without this guard two concurrent inserts for `vN` and `vN+1` could both
    /// read the same tail and fork the chain. A SQL-side hash (a scalar function)
    /// would let a lone INSERT...SELECT do it with no Rust read at all, but that
    /// is not D1-portable (D1 has no user-defined functions and SQLite has no
    /// built-in SHA-256), so the linkage is enforced with a pure-SQL guard
    /// instead: a stale tail read yields a guarded no-op (0 rows) and a retry,
    /// so no forked row is ever committed.
    fn insert_pending(
        conn: &Connection,
        version: u64,
        checksum: &str,
        lease_secs: i64,
    ) -> Result<bool> {
        // Chain onto the highest existing version (the tail). Gaps are fine: the
        // chain links ascending, whatever versions are present.
        let prev_hash = Self::tail_chain_hash(conn)?;
        Self::insert_pending_chained(conn, version, checksum, lease_secs, &prev_hash)
    }

    /// Single-statement guarded insert: chain `version` onto `expected_prev`, but
    /// only if `expected_prev` still equals the live tail's chain-hash. The
    /// `WHERE` compares `expected_prev` against `MAX(version)`'s `chain_hash`
    /// (genesis-guarded via `COALESCE`) atomically with the write, so there is no
    /// exploitable read-then-write window: if a concurrent distinct-version insert
    /// advanced the tail after our read, the guard fails, `0` rows are written,
    /// and we return `Ok(false)` for the caller to retry against the new tail.
    /// Transaction-free and standard SQL, so it is D1-portable.
    fn insert_pending_chained(
        conn: &Connection,
        version: u64,
        checksum: &str,
        lease_secs: i64,
        expected_prev: &str,
    ) -> Result<bool> {
        let chain_hash = chain_hash(version, checksum, expected_prev);
        let now = now_unix();
        let affected = conn.execute(
            &format!(
                "INSERT INTO \"{LEDGER_TABLE}\"
                     (version, checksum, status, applied_at, lease_expires_at,
                      prev_hash, chain_hash)
                 SELECT ?1, ?2, 'pending', ?3, ?4, ?5, ?6
                 WHERE COALESCE(
                     (SELECT chain_hash FROM \"{LEDGER_TABLE}\"
                      ORDER BY version DESC LIMIT 1),
                     ?7
                 ) = ?5"
            ),
            rusqlite::params![
                version as i64,
                checksum,
                now,
                now + lease_secs,
                expected_prev,
                chain_hash,
                GENESIS_PREV_HASH,
            ],
        )?;
        Ok(affected == 1)
    }

    /// Compare-and-set reclaim of an expired pending row. Returns whether this
    /// node won (exactly one row updated). The `WHERE` guard makes the CAS atomic
    /// without a transaction: only one racer can move the row off its old lease.
    fn reclaim_pending(conn: &Connection, version: u64, lease_secs: i64) -> Result<bool> {
        let now = now_unix();
        let affected = conn.execute(
            &format!(
                "UPDATE \"{LEDGER_TABLE}\"
                 SET applied_at = ?1, lease_expires_at = ?2
                 WHERE version = ?3 AND status = 'pending'
                   AND (lease_expires_at IS NULL OR lease_expires_at < ?1)"
            ),
            rusqlite::params![now, now + lease_secs, version as i64],
        )?;
        Ok(affected == 1)
    }

    /// Compare-and-set reclaim of a failed row back to pending. Returns whether
    /// this node won.
    fn reclaim_failed(conn: &Connection, version: u64, lease_secs: i64) -> Result<bool> {
        let now = now_unix();
        let affected = conn.execute(
            &format!(
                "UPDATE \"{LEDGER_TABLE}\"
                 SET status = 'pending', applied_at = ?1, lease_expires_at = ?2
                 WHERE version = ?3 AND status = 'failed'"
            ),
            rusqlite::params![now, now + lease_secs, version as i64],
        )?;
        Ok(affected == 1)
    }

    /// The chain-hash of the highest-versioned existing entry, or the genesis
    /// hash when the ledger is empty.
    ///
    /// Uses [`OptionalExtension::optional`] so that `None` means **only** "no rows
    /// yet" (a legitimate genesis) while a real query error propagates as `Err`.
    /// A tamper-evidence chain must never `.ok()`-swallow a DB error into a silent
    /// "empty chain" (MED 1): that would mask corruption as a fresh genesis.
    fn tail_chain_hash(conn: &Connection) -> Result<String> {
        let tail: Option<String> = conn
            .query_row(
                &format!(
                    "SELECT chain_hash FROM \"{LEDGER_TABLE}\"
                 ORDER BY version DESC LIMIT 1"
                ),
                [],
                |row| row.get(0),
            )
            .optional()?;
        Ok(tail.unwrap_or_else(|| GENESIS_PREV_HASH.to_string()))
    }
}

/// Compute a ledger entry's chain-hash.
///
/// `SHA-256(version_be || 0x00 || checksum || 0x00 || prev_hash)`. The NUL
/// separators keep the concatenation unambiguous. Only the immutable identity of
/// the entry (`version`, `checksum`) and the predecessor link feed the hash;
/// mutable columns (status, lease, applied_at) and the forward-compat columns are
/// deliberately excluded so legitimate transitions do not break the chain.
fn chain_hash(version: u64, checksum: &str, prev_hash: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(version.to_be_bytes());
    hasher.update([0u8]);
    hasher.update(checksum.as_bytes());
    hasher.update([0u8]);
    hasher.update(prev_hash.as_bytes());
    hex::encode(hasher.finalize())
}

/// Current wall-clock time as a Unix timestamp in seconds.
fn now_unix() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Whether a pending lease has expired relative to `now`. A `None` expiry (should
/// not occur for a live pending row) is treated as expired so it is reclaimable
/// rather than wedged.
fn lease_expired(lease_expires_at: Option<i64>, now: i64) -> bool {
    match lease_expires_at {
        Some(expiry) => expiry < now,
        None => true,
    }
}

/// Whether a rusqlite error is a `UNIQUE` constraint violation (the election
/// race-loser signal).
fn is_unique_violation(e: &rusqlite::Error) -> bool {
    matches!(
        e,
        rusqlite::Error::SqliteFailure(err, _) if err.extended_code == SQLITE_CONSTRAINT_UNIQUE
    )
}

/// Map a result row (in the fixed `SELECT` column order) to a [`LedgerEntry`].
fn row_to_entry(row: &rusqlite::Row<'_>) -> Result<LedgerEntry> {
    let status_raw: String = row.get(2)?;
    Ok(LedgerEntry {
        version: row.get::<_, i64>(0)? as u64,
        checksum: row.get(1)?,
        status: MigrationStatus::parse(&status_raw)?,
        applied_at: row.get(3)?,
        lease_expires_at: row.get(4)?,
        prev_hash: row.get(5)?,
        chain_hash: row.get(6)?,
        preimage_ref: row.get(7)?,
        schema_projection: row.get(8)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn() -> Connection {
        let conn = Connection::open_in_memory().expect("open in-memory db");
        Ledger::ensure_schema(&conn).expect("ensure schema");
        conn
    }

    /// Force a pending row's lease into the past, simulating a crashed applier /
    /// elapsed time without sleeping.
    fn expire_lease(conn: &Connection, version: u64) {
        conn.execute(
            &format!("UPDATE \"{LEDGER_TABLE}\" SET lease_expires_at = 0 WHERE version = ?1"),
            rusqlite::params![version as i64],
        )
        .expect("expire lease");
    }

    #[test]
    fn ensure_schema_is_idempotent() {
        let conn = conn();
        Ledger::ensure_schema(&conn).unwrap();
        Ledger::ensure_schema(&conn).unwrap();
    }

    #[test]
    fn forward_compat_columns_provisioned_and_null() {
        let conn = conn();
        assert_eq!(
            Ledger::try_elect(&conn, 1, "cksum1", 300).unwrap(),
            Election::Won
        );
        let entry = Ledger::entry(&conn, 1).unwrap().unwrap();
        assert_eq!(entry.preimage_ref, None);
        assert_eq!(entry.schema_projection, None);
    }

    #[test]
    fn election_elects_single_winner() {
        let conn = conn();
        // First claim wins; the row is now pending with a live lease.
        assert_eq!(
            Ledger::try_elect(&conn, 1, "c1", 300).unwrap(),
            Election::Won
        );
        // A second claim on the live-leased pending row is held off, not re-won.
        assert_eq!(
            Ledger::try_elect(&conn, 1, "c1", 300).unwrap(),
            Election::HeldByOther
        );
    }

    #[test]
    fn success_gate_skips_only_on_success() {
        let conn = conn();
        assert_eq!(
            Ledger::try_elect(&conn, 1, "c1", 300).unwrap(),
            Election::Won
        );
        Ledger::mark_success(&conn, 1).unwrap();
        assert_eq!(
            Ledger::try_elect(&conn, 1, "c1", 300).unwrap(),
            Election::AlreadyApplied
        );
        // current_version now observes v1.
        assert_eq!(Ledger::current_version(&conn).unwrap(), Some(1));
    }

    #[test]
    fn poison_pending_re_drives_never_skips() {
        // The sharpest AC: a crashed-mid-apply pending row must RE-DRIVE, not be
        // skipped, and must never advance current_version.
        let conn = conn();
        assert_eq!(
            Ledger::try_elect(&conn, 5, "c5", 300).unwrap(),
            Election::Won
        );
        // Crash: never marked success. current_version must not see v5.
        assert_eq!(Ledger::current_version(&conn).unwrap(), None);
        // Lease expires (elapsed time / crashed node).
        expire_lease(&conn, 5);
        // A fresh election reclaims and re-drives -- NOT AlreadyApplied.
        assert_eq!(
            Ledger::try_elect(&conn, 5, "c5", 300).unwrap(),
            Election::Won
        );
        assert_eq!(Ledger::current_version(&conn).unwrap(), None);
        // And after it finally succeeds, it is observable and skipped.
        Ledger::mark_success(&conn, 5).unwrap();
        assert_eq!(Ledger::current_version(&conn).unwrap(), Some(5));
        assert_eq!(
            Ledger::try_elect(&conn, 5, "c5", 300).unwrap(),
            Election::AlreadyApplied
        );
    }

    #[test]
    fn failed_row_re_drives() {
        // A failed row is reclaimable -- the gate skips only on success, never on
        // row-existence. Sits beside the poison-pending test as the second half of
        // "never skip on row-existence".
        let conn = conn();
        assert_eq!(
            Ledger::try_elect(&conn, 2, "c2", 300).unwrap(),
            Election::Won
        );
        Ledger::mark_failed(&conn, 2).unwrap();
        assert_eq!(Ledger::current_version(&conn).unwrap(), None);
        // Re-driven, not skipped.
        assert_eq!(
            Ledger::try_elect(&conn, 2, "c2", 300).unwrap(),
            Election::Won
        );
    }

    #[test]
    fn expired_pending_lease_is_reclaimable() {
        let conn = conn();
        assert_eq!(
            Ledger::try_elect(&conn, 1, "c1", 300).unwrap(),
            Election::Won
        );
        // Live lease -> held.
        assert_eq!(
            Ledger::try_elect(&conn, 1, "c1", 300).unwrap(),
            Election::HeldByOther
        );
        // Expired lease -> reclaimable.
        expire_lease(&conn, 1);
        assert_eq!(
            Ledger::try_elect(&conn, 1, "c1", 300).unwrap(),
            Election::Won
        );
    }

    #[test]
    fn current_version_tracks_highest_success() {
        let conn = conn();
        assert_eq!(Ledger::current_version(&conn).unwrap(), None);
        for v in 1..=3 {
            assert_eq!(
                Ledger::try_elect(&conn, v, &format!("c{v}"), 300).unwrap(),
                Election::Won
            );
            Ledger::mark_success(&conn, v).unwrap();
        }
        assert_eq!(Ledger::current_version(&conn).unwrap(), Some(3));
        // A later pending (unfinished) v4 does not advance the observable version.
        assert_eq!(
            Ledger::try_elect(&conn, 4, "c4", 300).unwrap(),
            Election::Won
        );
        assert_eq!(Ledger::current_version(&conn).unwrap(), Some(3));
    }

    #[test]
    fn chain_hash_links_entries() {
        let conn = conn();
        for v in 1..=3 {
            Ledger::try_elect(&conn, v, &format!("c{v}"), 300).unwrap();
            Ledger::mark_success(&conn, v).unwrap();
        }
        let entries = Ledger::entries(&conn).unwrap();
        assert_eq!(entries[0].prev_hash, GENESIS_PREV_HASH);
        assert_eq!(entries[1].prev_hash, entries[0].chain_hash);
        assert_eq!(entries[2].prev_hash, entries[1].chain_hash);
        Ledger::verify_chain(&conn).unwrap();
    }

    #[test]
    fn tamper_via_update_breaks_chain() {
        // An out-of-band UPDATE to a chained field (checksum) is detected.
        let conn = conn();
        for v in 1..=3 {
            Ledger::try_elect(&conn, v, &format!("c{v}"), 300).unwrap();
            Ledger::mark_success(&conn, v).unwrap();
        }
        Ledger::verify_chain(&conn).unwrap();
        conn.execute(
            &format!("UPDATE \"{LEDGER_TABLE}\" SET checksum = 'tampered' WHERE version = 1"),
            [],
        )
        .unwrap();
        let err = Ledger::verify_chain(&conn).unwrap_err();
        assert!(matches!(err, SyncError::LedgerTampered(_)));
        assert_eq!(err.exit_code(), 4);
    }

    #[test]
    fn tamper_via_middle_delete_breaks_chain() {
        // Deleting a middle entry orphans the successor's prev_hash link.
        let conn = conn();
        for v in 1..=3 {
            Ledger::try_elect(&conn, v, &format!("c{v}"), 300).unwrap();
            Ledger::mark_success(&conn, v).unwrap();
        }
        conn.execute(
            &format!("DELETE FROM \"{LEDGER_TABLE}\" WHERE version = 2"),
            [],
        )
        .unwrap();
        let err = Ledger::verify_chain(&conn).unwrap_err();
        assert!(matches!(err, SyncError::LedgerTampered(_)));
    }

    #[test]
    fn writing_forward_compat_columns_does_not_break_chain() {
        // preimage_ref / schema_projection are OUTSIDE the chain input; #274/#290
        // writing them later must not trip tamper detection.
        let conn = conn();
        for v in 1..=2 {
            Ledger::try_elect(&conn, v, &format!("c{v}"), 300).unwrap();
            Ledger::mark_success(&conn, v).unwrap();
        }
        conn.execute(
            &format!(
                "UPDATE \"{LEDGER_TABLE}\" \
                 SET preimage_ref = 'ref://x', schema_projection = '{{\"t\":1}}' \
                 WHERE version = 1"
            ),
            [],
        )
        .unwrap();
        Ledger::verify_chain(&conn).unwrap();
        let entry = Ledger::entry(&conn, 1).unwrap().unwrap();
        assert_eq!(entry.preimage_ref.as_deref(), Some("ref://x"));
        assert_eq!(entry.schema_projection.as_deref(), Some("{\"t\":1}"));
    }

    #[test]
    fn status_round_trips_through_storage() {
        let conn = conn();
        Ledger::try_elect(&conn, 1, "c1", 300).unwrap();
        assert_eq!(
            Ledger::entry(&conn, 1).unwrap().unwrap().status,
            MigrationStatus::Pending
        );
        Ledger::mark_failed(&conn, 1).unwrap();
        assert_eq!(
            Ledger::entry(&conn, 1).unwrap().unwrap().status,
            MigrationStatus::Failed
        );
    }

    #[test]
    fn insert_pending_derives_prev_from_live_tail() {
        // MED 2: the guarded single-statement insert chains onto the *current*
        // tail derived at write time, not a stale value, and verify_chain passes.
        let conn = conn();
        for v in 1..=2 {
            Ledger::try_elect(&conn, v, &format!("c{v}"), 300).unwrap();
            Ledger::mark_success(&conn, v).unwrap();
        }
        let tail = Ledger::entry(&conn, 2).unwrap().unwrap().chain_hash;
        // Direct insert of a fresh version chains off the live tail (v2).
        assert!(Ledger::insert_pending(&conn, 3, "c3", 300).unwrap());
        let v3 = Ledger::entry(&conn, 3).unwrap().unwrap();
        assert_eq!(v3.prev_hash, tail);
        assert_eq!(v3.chain_hash, chain_hash(3, "c3", &tail));
        Ledger::verify_chain(&conn).unwrap();
    }

    #[test]
    fn stale_tail_insert_is_rejected_by_cas() {
        // MED 2 (the core guard): an insert that chains off a prev-hash that no
        // longer matches the live tail is a no-op (0 rows), never a forked commit.
        let conn = conn();
        Ledger::try_elect(&conn, 1, "c1", 300).unwrap();
        Ledger::mark_success(&conn, 1).unwrap();
        let live_tail = Ledger::entry(&conn, 1).unwrap().unwrap().chain_hash;

        // A racer holding a stale/forged predecessor is rejected atomically.
        let stale = "0000000000000000000000000000000000000000000000000000000000000000";
        assert!(!Ledger::insert_pending_chained(&conn, 2, "c2", 300, stale).unwrap());
        assert!(Ledger::entry(&conn, 2).unwrap().is_none());

        // Chaining off the true tail succeeds.
        assert!(Ledger::insert_pending_chained(&conn, 2, "c2", 300, &live_tail).unwrap());
        assert_eq!(
            Ledger::entry(&conn, 2).unwrap().unwrap().prev_hash,
            live_tail
        );
        Ledger::verify_chain(&conn).unwrap();
    }

    #[test]
    fn concurrent_distinct_version_inserts_yield_consistent_chain() {
        // MED 2 end-to-end: two racers (vN and vN+1) both read the same tail, then
        // both try to append. The lower version wins the append first; the higher
        // version's stale-tail insert is rejected and re-derives against the new
        // tail, so the chain never forks. Ascending commit order mirrors migrate's
        // apply order (vN+1 is attempted only after vN lands).
        let conn = conn();
        Ledger::try_elect(&conn, 1, "c1", 300).unwrap();
        Ledger::mark_success(&conn, 1).unwrap();

        // Both racers captured the same stale tail (v1's chain-hash).
        let stale_tail = Ledger::tail_chain_hash(&conn).unwrap();

        // Racer for v2 wins the append first (chains off the shared tail).
        assert!(Ledger::insert_pending_chained(&conn, 2, "c2", 300, &stale_tail).unwrap());

        // Racer for v3 tries to chain off the now-stale tail: guard rejects it.
        assert!(!Ledger::insert_pending_chained(&conn, 3, "c3", 300, &stale_tail).unwrap());
        assert!(Ledger::entry(&conn, 3).unwrap().is_none());

        // Retry re-derives against the live tail (v2) and succeeds -- no fork.
        let fresh_tail = Ledger::tail_chain_hash(&conn).unwrap();
        assert!(Ledger::insert_pending_chained(&conn, 3, "c3", 300, &fresh_tail).unwrap());

        let entries = Ledger::entries(&conn).unwrap();
        assert_eq!(entries[1].prev_hash, entries[0].chain_hash);
        assert_eq!(entries[2].prev_hash, entries[1].chain_hash);
        // v3 chains off v2, NOT the shared stale tail -- the fork is prevented.
        assert_ne!(entries[2].prev_hash, stale_tail);
        Ledger::verify_chain(&conn).unwrap();
    }
}

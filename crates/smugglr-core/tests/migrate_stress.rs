//! Migrations against databases with volume, history and a chain behind them.
//!
//! Everything forger proved before this file proved it about databases that
//! were seconds old and held one or two rows. That is enough to observe that a
//! cascade cascades or a trigger fires -- a correctness probe -- and it is not
//! data and it is not a history. Nothing in the suite would have noticed a
//! rebuild that corrupts row 4,000, mishandles a NULL in a column the seed
//! leaves populated, or truncates a value at a length no fixture reaches.
//!
//! smugglr#378. The transformation under test is always
//! [`apply_migration`] -- the real composing forward apply, checksum verify and
//! ledger and election and lint included. No SQL in this file is executed
//! against a database the assertions then read as migrated output.
//!
//! # What "aged" has to mean, and why it cannot be faked
//!
//! An old database is not the same database with more rows in it. It is one
//! bearing the marks of what happened to it: a populated ledger with a real
//! chain across it, an autoincrement high-water mark above `max(rowid)` that
//! only deletes can produce, `CREATE` text already rewritten by an earlier
//! rebuild, and objects created at different points rather than all at once.
//!
//! So [`AgedDatabase`] does not *write* those marks, it *earns* them: it runs
//! real migrations and lets the marks fall out. A helper that hand-wrote a
//! ledger row would be a fixture wearing a history's clothes, and every test
//! built on it would be green about a database that never aged. That is why
//! [`the_aged_database_is_actually_old`] exists: every other test here rests on
//! that constructor, so the age it claims to produce is measured rather than
//! trusted.
//!
//! # One constraint discovered rather than assumed
//!
//! The chain has to **end** with the failed entry. `Ledger::current_version`
//! counts only `success`, so a failed row at version N leaves the observable
//! version at N-1, and the next apply re-derives N, finds the failed row and
//! reclaims it into `pending` then `success`. A `failed` status therefore does
//! not survive a later apply of the same slot -- which is not a limitation to
//! work around but exactly what a real aborted migration leaves behind, and the
//! state smugglr#328's reclaim-path checksum defect lives in.

use rusqlite::Connection;

use smugglr_core::migrate::driver::{apply_migration, ApplyOptions};
use smugglr_core::migrate::ledger::{Ledger, MigrationStatus};
use smugglr_core::migrate::{
    ChecksummedManifest, ClassifiedOp, Column, ColumnKind, Flags, Manifest, Op,
};

// ---------------------------------------------------------------------------
// The names the whole file shares
// ---------------------------------------------------------------------------

/// The parent table. Its children declare `ON DELETE CASCADE`, so an aged
/// database carries a referential action through every rebuild in its history
/// -- the construct smugglr#341 was losing.
const CUSTOMERS: &str = "customers";

/// The table that gets rebuilt. Carries the autoincrement key, the unique
/// column whose drop forces the rebuild, and the foreign key.
const ORDERS: &str = "orders";

/// The column whose `UNIQUE` makes SQLite refuse the direct
/// `ALTER TABLE ... DROP COLUMN`, sending the op through the rebuild.
///
/// Reaching the rebuild is not automatic and this is the lever: `drop_column`
/// tries the direct `ALTER` first and only rebuilds when SQLite turns it down.
/// A migration dropping an ordinary column exercises the path that was never
/// broken.
const REBUILD_FORCER: &str = "external_ref";

/// Added by the first migration in the chain.
const ADDED_IN_V1: &str = "note";

/// The index the third migration creates, after the rebuild rather than with
/// the table -- so the aged database's objects have different birthdays.
const LATE_INDEX: &str = "orders_by_customer";

/// The trigger created between migrations, as application code does rather
/// than a migration op (there is no trigger op in the manifest vocabulary).
const AUDIT_TRIGGER: &str = "orders_audit";

// ---------------------------------------------------------------------------
// The aged database
// ---------------------------------------------------------------------------

/// A database that went through a chain rather than one told it had.
struct AgedDatabase {
    conn: Connection,
    /// The version the last *successful* migration settled at.
    last_success: u64,
    /// The version the deliberately failing migration claimed.
    failed_version: u64,
    /// The `orders` DDL as it stood before any rebuild, kept so a test can show
    /// the stored text really was rewritten rather than asserting it was.
    ddl_before_rebuild: String,
}

impl AgedDatabase {
    /// Live a history, and return the database that has one.
    fn build() -> Self {
        let conn = Connection::open_in_memory().expect("an in-memory database");
        conn.execute_batch("PRAGMA foreign_keys = ON;")
            .expect("enforcement on");

        // The base schema is raw SQL on purpose, and it is the honest shape:
        // an aged database predates the migration tool that later runs against
        // it. It is also required -- AUTOINCREMENT has no spelling in the
        // manifest's `Constraint` vocabulary, so `Op::CreateTable` cannot
        // originate one, and without it there is no `sqlite_sequence` row to
        // carry a high-water mark.
        conn.execute_batch(&format!(
            "CREATE TABLE {CUSTOMERS} (
                 id INTEGER PRIMARY KEY,
                 name TEXT NOT NULL
             );
             CREATE TABLE {ORDERS} (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 customer_id INTEGER REFERENCES {CUSTOMERS}(id) ON DELETE CASCADE,
                 {REBUILD_FORCER} TEXT UNIQUE,
                 amount INTEGER
             );"
        ))
        .expect("the base schema stands up");

        // Rows, then deletes. The high-water mark is what deletes leave behind:
        // sqlite_sequence keeps the largest key ever issued, so after removing
        // the tail it stands above max(rowid) and no amount of inserting alone
        // would produce that.
        conn.execute_batch(&format!(
            "INSERT INTO {CUSTOMERS} (id, name) VALUES (1, 'kept'), (2, 'also kept');"
        ))
        .expect("customers seed");
        for i in 1..=200 {
            conn.execute(
                &format!(
                    "INSERT INTO {ORDERS} (customer_id, {REBUILD_FORCER}, amount) \
                     VALUES (?1, ?2, ?3)"
                ),
                rusqlite::params![1 + (i % 2), format!("ref-{i}"), i * 10],
            )
            .expect("orders seed");
        }
        conn.execute(&format!("DELETE FROM {ORDERS} WHERE id > 150"), [])
            .expect("the tail goes");

        let mut aged = AgedDatabase {
            conn,
            last_success: 0,
            failed_version: 0,
            ddl_before_rebuild: String::new(),
        };

        // v1 -- additive. Ordinary, and it is here so the chain is not made
        // entirely of the interesting op.
        aged.apply_ok(
            "v1 add a column",
            vec![ClassifiedOp::new(Op::AddColumn {
                table: ORDERS.into(),
                column: Column {
                    name: ADDED_IN_V1.into(),
                    kind: ColumnKind::Text,
                    constraints: Vec::new(),
                    tags: Vec::new(),
                },
            })],
        );

        // Application code, between migrations. Triggers have no op in the
        // manifest vocabulary, and a database whose triggers all arrived with
        // its tables is not one anybody operates.
        aged.conn
            .execute_batch(&format!(
                "CREATE TABLE order_audit (id INTEGER PRIMARY KEY, order_id INTEGER);
                 CREATE TRIGGER {AUDIT_TRIGGER} AFTER INSERT ON {ORDERS}
                 BEGIN
                     INSERT INTO order_audit (order_id) VALUES (new.id);
                 END;"
            ))
            .expect("the trigger lands");

        aged.ddl_before_rebuild = aged.ddl_of(ORDERS);

        // v2 -- the rebuild. Dropping the UNIQUE column is refused as a direct
        // ALTER, so this reconstructs the table: it rewrites the stored CREATE
        // text, and it has to carry the foreign key's ON DELETE CASCADE and
        // replay the trigger.
        aged.apply_ok(
            "v2 drop the unique column, forcing a rebuild",
            vec![ClassifiedOp::new(Op::DropColumn {
                table: ORDERS.into(),
                column: REBUILD_FORCER.into(),
            })],
        );

        // v3 -- an index, created after the rebuild rather than with the table.
        aged.apply_ok(
            "v3 create an index",
            vec![ClassifiedOp::new(Op::CreateIndex {
                name: LATE_INDEX.into(),
                table: ORDERS.into(),
                columns: vec!["customer_id".into()],
                unique: false,
            })],
        );

        // v4 -- and the migration that did not make it. Dropping a primary-key
        // column is refused by the pre-image capturer on the apply loop's
        // write-ahead hook, *after* the election is won, which is what puts a
        // `failed` row in the ledger rather than erroring before it is touched.
        aged.failed_version = aged.apply_expecting_failure(
            "v4 drop the primary key",
            vec![ClassifiedOp::new(Op::DropColumn {
                table: ORDERS.into(),
                column: "id".into(),
            })],
        );

        aged
    }

    /// Apply a manifest that must succeed, and remember the version it settled.
    fn apply_ok(&mut self, what: &str, up: Vec<ClassifiedOp>) {
        let sealed = seal(up);
        let outcome = apply_migration(&self.conn, &sealed, &ApplyOptions::default())
            .unwrap_or_else(|error| panic!("{what}: the migration did not apply: {error}"));
        self.last_success = outcome.version;
    }

    /// Apply a manifest that must fail, and return the version it claimed.
    ///
    /// The version is derived rather than read off the outcome, because there
    /// is no outcome -- the call returns `Err`. It is the next one after the
    /// last success, which is exactly the slot the ledger's failed row holds.
    fn apply_expecting_failure(&mut self, what: &str, up: Vec<ClassifiedOp>) -> u64 {
        let sealed = seal(up);
        let result = apply_migration(&self.conn, &sealed, &ApplyOptions::default());
        assert!(
            result.is_err(),
            "{what}: this migration is supposed to fail, and it applied cleanly -- the chain \
             would then have no failed entry and every test reading one would be green about a \
             database that never had a failure in it"
        );
        self.last_success + 1
    }

    /// The stored `CREATE` text of a table.
    fn ddl_of(&self, table: &str) -> String {
        self.conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
                [table],
                |r| r.get(0),
            )
            .unwrap_or_else(|error| panic!("{table} has no stored DDL: {error}"))
    }

    /// The `sqlite_sequence` high-water mark for a table.
    fn high_water(&self, table: &str) -> i64 {
        self.conn
            .query_row(
                "SELECT seq FROM sqlite_sequence WHERE name = ?1",
                [table],
                |r| r.get(0),
            )
            .unwrap_or_else(|error| panic!("{table} has no sqlite_sequence row: {error}"))
    }

    fn count(&self, sql: &str) -> i64 {
        self.conn
            .query_row(sql, [], |r| r.get(0))
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
    }
}

/// Seal a manifest the way the CLI would one read off disk.
///
/// `version` is the generator's hardcoded value and the driver overrides it;
/// `target_schema` is opaque for 0.5.0. `flags` is left default deliberately --
/// nothing on the apply path reads it, so setting it would suggest a gate that
/// does not exist.
fn seal(up: Vec<ClassifiedOp>) -> ChecksummedManifest {
    ChecksummedManifest::seal(Manifest {
        version: 1,
        target_schema: "opaque".into(),
        up,
        down: Vec::new(),
        preimage: None,
        flags: Flags::default(),
        author: None,
    })
    .expect("the manifest seals")
}

// ---------------------------------------------------------------------------
// The age is real
// ---------------------------------------------------------------------------

/// The constructor produced a database with a history, and each mark is checked
/// rather than assumed.
///
/// This test exists because every other test in this file rests on it. A helper
/// that quietly produced a *young* database -- one whose migrations no-oped,
/// whose rebuild never ran, whose failure silently succeeded -- would make
/// everything downstream green for the wrong reason, which is the failure mode
/// this whole crate was built to eliminate. So the age is a measurement.
#[test]
fn the_aged_database_is_actually_old() {
    let aged = AgedDatabase::build();

    // -- the ledger has a chain, and it is a real one --------------------
    let entries = Ledger::entries(&aged.conn).expect("the ledger reads back");
    assert!(
        entries.len() >= 4,
        "a chain of at least four migrations was applied; the ledger holds {}: {entries:?}",
        entries.len()
    );
    Ledger::verify_chain(&aged.conn).expect("the chain hashes across the entries");

    // -- three succeeded and the last one did not ------------------------
    let succeeded = entries
        .iter()
        .filter(|e| e.status == MigrationStatus::Success)
        .count();
    assert!(
        succeeded >= 3,
        "at least three migrations settled successfully; {succeeded} did: {entries:?}"
    );
    let failed = entries
        .iter()
        .find(|e| e.status == MigrationStatus::Failed)
        .unwrap_or_else(|| {
            panic!(
                "the chain has no failed entry, so the reclaim path smugglr#328 lives on is \
                 unreachable from this database: {entries:?}"
            )
        });
    assert_eq!(
        failed.version, aged.failed_version,
        "the failed entry is the version the failing migration claimed"
    );

    // -- the autoincrement high-water mark is above max(rowid) -----------
    // Only deletes produce this. A database that was merely inserted into has
    // seq == max(rowid) and is indistinguishable from a fresh one here.
    let seq = aged.high_water(ORDERS);
    let max_rowid = aged.count(&format!("SELECT max(id) FROM {ORDERS}"));
    assert!(
        seq > max_rowid,
        "sqlite_sequence for {ORDERS} is {seq} and max(id) is {max_rowid}; without a gap this \
         database has never had a row deleted and is not aged in the way that matters"
    );

    // -- a rebuild really rewrote the stored CREATE text -----------------
    let after = aged.ddl_of(ORDERS);
    assert_ne!(
        after, aged.ddl_before_rebuild,
        "the stored DDL is unchanged, so the rebuild never ran and the database's CREATE text is \
         still the one it was authored with"
    );
    assert!(
        !after.contains(REBUILD_FORCER),
        "the dropped column is still in the stored DDL: {after}"
    );
    assert!(
        after.contains("AUTOINCREMENT"),
        "the rebuild lost AUTOINCREMENT, so the high-water mark above is about a table that no \
         longer has a sequence: {after}"
    );

    // -- objects have different birthdays --------------------------------
    let index_exists = aged.count(&format!(
        "SELECT count(*) FROM sqlite_master WHERE type = 'index' AND name = '{LATE_INDEX}'"
    ));
    assert_eq!(index_exists, 1, "the late index survived to the end");
    let trigger_exists = aged.count(&format!(
        "SELECT count(*) FROM sqlite_master WHERE type = 'trigger' AND name = '{AUDIT_TRIGGER}'"
    ));
    assert_eq!(
        trigger_exists, 1,
        "the trigger created between migrations survived the rebuild that came after it"
    );

    // -- and it still holds its rows -------------------------------------
    let rows = aged.count(&format!("SELECT count(*) FROM {ORDERS}"));
    assert_eq!(rows, 150, "the surviving orders are still there");
}

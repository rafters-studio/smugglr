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

// ---------------------------------------------------------------------------
// Values that are adversarial in the ways real data is
// ---------------------------------------------------------------------------

/// How many rows the volume test migrates.
///
/// Large enough that a defect at one row in a thousand has somewhere to hide,
/// and small enough that the suite stays runnable. Volume is the least
/// interesting half of "real data" -- what matters is that the values below
/// repeat through it.
const ROWS: i64 = 4_000;

/// One value, and what it is here to catch.
///
/// A row count cannot see any of these. A rebuild that truncates at a length no
/// fixture reaches, coerces a blob that happens to be valid UTF-8, or loses the
/// distinction between an empty string and NULL, keeps every row and changes
/// what is in them.
struct Adversarial {
    /// What the value is, for a failure message that names it.
    what: &'static str,
    /// The value, as the SQL literal that produces it.
    literal: &'static str,
}

/// The corpus, applied round-robin so every value appears many times across the
/// volume rather than once at a known offset.
///
/// `pk_text_expr`'s delimiters are in here deliberately. The engine escapes `\`
/// and `|` when it renders a composite key as text, and that escaping exists
/// because the delimiter is reachable from data -- so data containing both is
/// the case the escaping was written for and the one nothing was feeding it.
const ADVERSARIAL: &[Adversarial] = &[
    Adversarial {
        what: "NULL",
        literal: "NULL",
    },
    Adversarial {
        what: "the empty string, which is not NULL",
        literal: "''",
    },
    Adversarial {
        what: "a single space, which trims to empty",
        literal: "' '",
    },
    Adversarial {
        what: "combining characters that normalise differently",
        literal: "'e\u{0301}cole'",
    },
    Adversarial {
        what: "a four-byte emoji",
        literal: "'x\u{1F680}y'",
    },
    Adversarial {
        what: "an embedded NUL-ish escape that is not a NUL",
        literal: "'a\\0b'",
    },
    Adversarial {
        what: "a single quote, doubled to escape it",
        literal: "'it''s'",
    },
    Adversarial {
        what: "a double quote, which SQLite may read as an identifier",
        literal: "'say \"hi\"'",
    },
    Adversarial {
        what: "pk_text_expr's pipe delimiter",
        literal: "'a|b'",
    },
    Adversarial {
        what: "pk_text_expr's backslash escape",
        literal: "'a\\b'",
    },
    Adversarial {
        what: "both delimiters, the ambiguity the escaping exists for",
        literal: "'a\\|b'",
    },
    Adversarial {
        what: "a newline inside a value",
        literal: "'line1' || char(10) || 'line2'",
    },
    Adversarial {
        what: "a blob that is valid UTF-8",
        literal: "CAST('text' AS BLOB)",
    },
    Adversarial {
        what: "a blob that is not valid UTF-8",
        literal: "x'ff00fe'",
    },
    Adversarial {
        what: "the empty blob",
        literal: "x''",
    },
    Adversarial {
        what: "i64::MAX",
        literal: "9223372036854775807",
    },
    Adversarial {
        what: "i64::MIN",
        literal: "-9223372036854775808",
    },
    Adversarial {
        what: "negative zero, which equals zero and is not it",
        literal: "-0.0",
    },
    Adversarial {
        what: "a subnormal float",
        literal: "5e-324",
    },
    Adversarial {
        what: "a float that is not exactly representable",
        literal: "0.1",
    },
    Adversarial {
        what: "a long value, past any fixture length",
        literal: "printf('%.*c', 8000, 'x')",
    },
];

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

// ---------------------------------------------------------------------------
// Volume, compared by value
// ---------------------------------------------------------------------------

/// Every value in the table before the migration is still there after it,
/// compared by value rather than by count.
///
/// A row count is what the existing suite could already do, and it cannot see
/// the failures that matter: a rebuild that truncates at a length no fixture
/// reaches, coerces a blob that happens to be valid UTF-8, resolves a typeless
/// column to TEXT, or loses the distinction between an empty string and NULL
/// keeps every row and changes what is in them.
///
/// So the comparison is a full dump of both sides. The values are the
/// [`ADVERSARIAL`] corpus applied round-robin across [`ROWS`] rows, so each
/// shape appears many times and at unpredictable offsets rather than once where
/// a reader would look for it.
///
/// The migration is a `DROP COLUMN` forced through the rebuild, because the
/// rebuild is the path that copies every row and therefore the path where a
/// value can change on the way.
#[test]
fn a_populated_table_survives_a_rebuild_value_for_value() {
    let conn = Connection::open_in_memory().expect("an in-memory database");
    conn.execute_batch(
        "CREATE TABLE payload (
             id INTEGER PRIMARY KEY,
             ordinary TEXT,
             untyped,
             number,
             chunk BLOB,
             forcer TEXT UNIQUE
         );",
    )
    .expect("the schema stands up");

    // Round-robin the corpus through every column that can hold it, so the
    // same shape lands in a typed column, a typeless one and a blob column.
    for row in 0..ROWS {
        let a = &ADVERSARIAL[(row as usize) % ADVERSARIAL.len()];
        let b = &ADVERSARIAL[((row as usize) + 7) % ADVERSARIAL.len()];
        let c = &ADVERSARIAL[((row as usize) + 13) % ADVERSARIAL.len()];
        conn.execute_batch(&format!(
            "INSERT INTO payload (id, ordinary, untyped, number, chunk, forcer) \
             VALUES ({row}, {}, {}, {}, {}, 'f{row}');",
            a.literal, b.literal, c.literal, a.literal
        ))
        .unwrap_or_else(|error| panic!("row {row} with {}: {error}", a.what));
    }

    let before = dump(
        &conn,
        "payload",
        &["id", "ordinary", "untyped", "number", "chunk"],
    );
    assert_eq!(before.len(), ROWS as usize, "every row was seeded");

    let sealed = seal(vec![ClassifiedOp::new(Op::DropColumn {
        table: "payload".into(),
        column: "forcer".into(),
    })]);
    apply_migration(&conn, &sealed, &ApplyOptions::default()).expect("the migration applies");

    let after = dump(
        &conn,
        "payload",
        &["id", "ordinary", "untyped", "number", "chunk"],
    );

    assert_eq!(
        after.len(),
        before.len(),
        "the rebuild changed the row count, which is the only failure a count would have caught"
    );

    // Named per row and per column, because "the dumps differ" over 4,000 rows
    // is a failure nobody can act on.
    for (row, (was, now)) in before.iter().zip(after.iter()).enumerate() {
        for (column, (left, right)) in was.iter().zip(now.iter()).enumerate() {
            assert_eq!(
                left,
                right,
                "row {row}, column {:?}: the rebuild changed the value from {left:?} to {right:?}",
                ["id", "ordinary", "untyped", "number", "chunk"][column]
            );
        }
    }
}

/// Every row of a table as typed values, ordered so two dumps are comparable.
///
/// `ValueRef` rather than a `String` rendering: the whole point is to notice a
/// value whose TYPE changed -- an integer that came back as text, a blob that
/// came back as a string -- and rendering both sides to text is exactly how
/// that goes unnoticed.
fn dump(conn: &Connection, table: &str, columns: &[&str]) -> Vec<Vec<rusqlite::types::Value>> {
    let list = columns
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let mut stmt = conn
        .prepare(&format!("SELECT {list} FROM \"{table}\" ORDER BY \"id\""))
        .expect("the dump query prepares");
    let rows = stmt
        .query_map([], |r| {
            (0..columns.len())
                .map(|i| r.get::<_, rusqlite::types::Value>(i))
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .expect("the dump runs")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("every row reads back");
    rows
}

/// The dump distinguishes values a text rendering would collapse.
///
/// This is what makes the comparison above stronger than a row count, so it is
/// asserted rather than assumed: an integer `1` and the text `'1'` are
/// different values, and a rebuild that turned one into the other -- which is
/// exactly what resolving a typeless column to TEXT does -- must not compare
/// equal. Rendering both sides to `String` before comparing is the natural way
/// to write this test and the way that would miss its own subject.
#[test]
fn the_dump_notices_a_value_whose_type_changed() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE a (id INTEGER PRIMARY KEY, v);
         CREATE TABLE b (id INTEGER PRIMARY KEY, v);
         INSERT INTO a (id, v) VALUES (1, 1), (2, x'00'), (3, '');
         INSERT INTO b (id, v) VALUES (1, '1'), (2, CAST(x'00' AS TEXT)), (3, NULL);",
    )
    .unwrap();

    let left = dump(&conn, "a", &["id", "v"]);
    let right = dump(&conn, "b", &["id", "v"]);

    assert_ne!(
        left[0], right[0],
        "an integer is not the text of that integer"
    );
    assert_ne!(
        left[1], right[1],
        "a blob is not the string with those bytes"
    );
    assert_ne!(left[2], right[2], "the empty string is not NULL");
}

// ---------------------------------------------------------------------------
// A chain, against a database built directly at the final version
// ---------------------------------------------------------------------------

/// Three migrations applied in sequence produce the same database as one built
/// at the final version and populated identically.
///
/// The interesting failures in a migration engine are cumulative -- step three
/// mishandles what step two left behind -- and a single-step harness cannot see
/// any of them. This is the differential shape with a chain on one side: the
/// arms differ only in HOW they arrived, so anything they disagree about is
/// something the chain did.
///
/// The comparison is by value and by schema. Schema alone would miss a chain
/// that arrived at the right shape with the wrong contents; values alone would
/// miss one that arrived at the right contents with a column declared
/// differently.
#[test]
fn a_three_step_chain_arrives_where_a_direct_build_does() {
    let migrated = Connection::open_in_memory().unwrap();
    migrated.execute_batch(START_SCHEMA).unwrap();
    seed_chain_rows(&migrated, true);

    // Dump between steps, not only at the end. A chain's interesting failures
    // are cumulative -- step three mishandles what step two left behind -- and
    // comparing only the final state can say a value is wrong without saying
    // WHICH step made it wrong, which is the half an operator needs.
    //
    // Compared over the columns every step is supposed to leave alone, so a
    // difference is always a step exceeding what it declared.
    let untouched = ["id", "ordinary", "untyped", "number", "chunk"];
    let mut previous = dump(&migrated, "payload", &untouched);
    for (index, ops) in chain().into_iter().enumerate() {
        let step = index + 1;
        let what = STEP_NAMES[index];
        let sealed = seal(ops);
        apply_migration(&migrated, &sealed, &ApplyOptions::default())
            .unwrap_or_else(|error| panic!("step {step} ({what}) did not apply: {error}"));

        let now = dump(&migrated, "payload", &untouched);
        assert_eq!(
            now.len(),
            previous.len(),
            "step {step} ({what}) on payload changed the row count from {} to {}",
            previous.len(),
            now.len()
        );
        for (row, (before, after)) in previous.iter().zip(now.iter()).enumerate() {
            for (column, (left, right)) in before.iter().zip(after.iter()).enumerate() {
                assert_eq!(
                    left, right,
                    "step {step} ({what}) on payload, row {row}, column {:?}: the value changed \
                     from {left:?} to {right:?}, and no step in this chain declares that",
                    untouched[column]
                );
            }
        }
        previous = now;
    }

    // The other arm: the final shape, built directly, populated the same way.
    let direct = Connection::open_in_memory().unwrap();
    direct.execute_batch(FINAL_SCHEMA).unwrap();
    seed_chain_rows(&direct, false);

    let cols = ["id", "ordinary", "untyped", "number", "chunk", "note"];
    let from_chain = dump(&migrated, "payload", &cols);
    let from_scratch = dump(&direct, "payload", &cols);

    assert_eq!(
        from_chain.len(),
        from_scratch.len(),
        "the arms hold different row counts"
    );
    for (row, (chained, built)) in from_chain.iter().zip(from_scratch.iter()).enumerate() {
        for (column, (left, right)) in chained.iter().zip(built.iter()).enumerate() {
            assert_eq!(
                left, right,
                "row {row}, column {:?}: the chain arrived at {left:?} where a direct build \
                 arrives at {right:?}",
                cols[column]
            );
        }
    }

    // ...and at the same shape. `table_xinfo` rather than `table_info` so a
    // generated column would be compared too.
    assert_eq!(
        declared_columns(&migrated, "payload"),
        declared_columns(&direct, "payload"),
        "the arms declare different columns"
    );
}

/// The shape the chain starts from.
const START_SCHEMA: &str = "CREATE TABLE payload (
     id INTEGER PRIMARY KEY,
     ordinary TEXT,
     untyped,
     number,
     chunk BLOB,
     forcer TEXT UNIQUE
 );";

/// The shape it should arrive at -- start, plus `note`, minus `forcer`.
///
/// Written out rather than derived, and that is the one hand-authored thing
/// here: it is the independent statement of where the chain claims to go. A
/// derived target would agree with the chain by construction and the comparison
/// would be with itself.
const FINAL_SCHEMA: &str = "CREATE TABLE payload (
     id INTEGER PRIMARY KEY,
     ordinary TEXT,
     untyped,
     number,
     chunk BLOB,
     note TEXT
 );";

/// What each step is, for a failure that names it rather than numbering it.
const STEP_NAMES: [&str; 3] = [
    "ADD COLUMN note",
    "DROP COLUMN forcer, through the rebuild",
    "CREATE INDEX payload_by_number",
];

/// The three steps. Each is a real manifest through the real driver.
fn chain() -> Vec<Vec<ClassifiedOp>> {
    vec![
        // 1 -- additive.
        vec![ClassifiedOp::new(Op::AddColumn {
            table: "payload".into(),
            column: Column {
                name: "note".into(),
                kind: ColumnKind::Text,
                constraints: Vec::new(),
                tags: Vec::new(),
            },
        })],
        // 2 -- the rebuild. `forcer` is UNIQUE, so the direct ALTER is refused
        // and every row is copied through the reconstruction.
        vec![ClassifiedOp::new(Op::DropColumn {
            table: "payload".into(),
            column: "forcer".into(),
        })],
        // 3 -- an index, created on a table that has already been rebuilt once.
        vec![ClassifiedOp::new(Op::CreateIndex {
            name: "payload_by_number".into(),
            table: "payload".into(),
            columns: vec!["number".into()],
            unique: false,
        })],
    ]
}

/// Populate an arm identically. `with_forcer` is the only difference, because
/// the pre-migration shape has a column the post-migration shape does not.
fn seed_chain_rows(conn: &Connection, with_forcer: bool) {
    for row in 0..500i64 {
        let a = &ADVERSARIAL[(row as usize) % ADVERSARIAL.len()];
        let b = &ADVERSARIAL[((row as usize) + 5) % ADVERSARIAL.len()];
        let sql = if with_forcer {
            format!(
                "INSERT INTO payload (id, ordinary, untyped, number, chunk, forcer) \
                 VALUES ({row}, {}, {}, {}, {}, 'f{row}');",
                a.literal, b.literal, a.literal, b.literal
            )
        } else {
            format!(
                "INSERT INTO payload (id, ordinary, untyped, number, chunk) \
                 VALUES ({row}, {}, {}, {}, {});",
                a.literal, b.literal, a.literal, b.literal
            )
        };
        conn.execute_batch(&sql)
            .unwrap_or_else(|error| panic!("row {row} with {}: {error}", a.what));
    }
}

/// Declared columns in order, generated ones included.
fn declared_columns(conn: &Connection, table: &str) -> Vec<String> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_xinfo(\"{table}\")"))
        .unwrap();
    stmt.query_map([], |r| r.get::<_, String>(1))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
}

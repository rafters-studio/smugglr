//! Standing a database up and tearing it down, deterministically.
//!
//! # Why the fixture owns the connection
//!
//! On Windows an open handle blocks deletion of the file it points at. A
//! fixture that hands out a `Connection` and separately owns a temp directory
//! is one early `drop` away from a cleanup failure that reproduces on exactly
//! one of three CI platforms -- the worst debugging shape available.
//!
//! So the connection is not handed out, it is lent: [`Fixture::conn`] returns
//! a borrow whose lifetime is the fixture's, and [`Fixture`]'s own [`Drop`]
//! closes the connection before releasing the directory. The ordering is a
//! statement in the code rather than a convention someone has to remember, and
//! because `Drop` runs while unwinding, it holds through a panicking test too.
//!
//! # Why both backings
//!
//! In memory is fast enough for schema comparison and leaves nothing behind by
//! construction. Anything that reaches the filesystem -- `VACUUM INTO`, a
//! file swap, byte-level inspection -- needs a real file, and choosing per
//! call site rather than globally is what keeps the fast case fast.
//!
//! # What forger does not do
//!
//! It sets no pragmas, and that is worth stating precisely rather than
//! generally. `rusqlite`'s bundled SQLite is compiled with
//! `SQLITE_DEFAULT_FOREIGN_KEYS=1`, so a fixture connection enforces foreign
//! keys immediately -- unlike the `sqlite3` shell, where the default is off.
//! forger neither turns that on nor off: a fixture that quietly configured the
//! connection would be testing its own defaults rather than the caller's
//! behaviour, and a caller who needs the other setting says so on the
//! connection it is handed.

use std::path::{Path, PathBuf};

use rusqlite::Connection;
use tempfile::TempDir;

use crate::error::{BoxError, ForgeError};
use crate::schema::Schema;

/// Where a fixture's database lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backing {
    /// `:memory:`. Nothing to clean up, nothing to inspect on disk.
    Memory,
    /// A real file inside a temp directory the fixture owns.
    File,
}

impl Backing {
    /// Every backing, for iterating.
    ///
    /// Read by [`boundary`](crate::boundary), which derives the execution paths
    /// forger exercises from the backings it can stand a database up on. A
    /// backing that is not listed here is a path the boundary will not claim,
    /// which understates coverage rather than overstating it -- the direction
    /// this crate errs in on purpose.
    pub const ALL: [Backing; 2] = [Backing::Memory, Backing::File];
}

/// How a fixture is brought to a state.
///
/// One method takes all three, and the symmetry is load-bearing rather than
/// cosmetic: a differential oracle builds one arm by applying the caller's
/// transformation and the other by plain DDL, then compares them, without
/// knowing anything about either.
pub enum Route<'a> {
    /// Apply a schema's rendered DDL.
    Schema(&'a Schema),
    /// Apply SQL verbatim -- several statements are fine.
    Ddl(&'a str),
    /// Run a caller-supplied transformation.
    ///
    /// `&mut Connection` rather than `&Connection` because
    /// `Connection::transaction` takes `&mut self`, and a transformation that
    /// cannot open a transaction cannot do the multi-statement work this
    /// exists for. The error is boxed and type-erased so forger can accept any
    /// consumer's error type without naming it -- forger knows nothing about
    /// what the transformation is or which crate it came from.
    Transform(&'a mut dyn FnMut(&mut Connection) -> Result<(), BoxError>),
}

/// A database, its connection, and (on [`Backing::File`]) the directory
/// holding it.
///
/// Not `Clone` and not `Send`-through-a-handle: there is exactly one owner,
/// and when it goes away the database goes away with it.
pub struct Fixture {
    // Held as Options only so `Drop` and `close` can take them; both are
    // present for the whole observable life of the fixture.
    conn: Option<Connection>,
    dir: Option<TempDir>,
    path: Option<PathBuf>,
}

impl Fixture {
    /// Create an empty database on the given backing.
    pub fn new(backing: Backing) -> Result<Self, ForgeError> {
        match backing {
            Backing::Memory => Ok(Self {
                conn: Some(Connection::open_in_memory()?),
                dir: None,
                path: None,
            }),
            Backing::File => {
                let dir = TempDir::new()?;
                let path = dir.path().join("forge.db");
                let conn = Connection::open(&path)?;
                Ok(Self {
                    conn: Some(conn),
                    dir: Some(dir),
                    path: Some(path),
                })
            }
        }
    }

    /// Borrow the connection.
    pub fn conn(&self) -> &Connection {
        self.conn.as_ref().expect("fixture connection is open")
    }

    /// Borrow the connection mutably, which is what opening a transaction
    /// needs.
    pub fn conn_mut(&mut self) -> &mut Connection {
        self.conn.as_mut().expect("fixture connection is open")
    }

    /// The database file, or `None` on [`Backing::Memory`].
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Bring the database to a state, by whichever route.
    pub fn bring_to(&mut self, route: Route<'_>) -> Result<(), ForgeError> {
        match route {
            Route::Schema(schema) => {
                // Validate first: a schema that renders to DDL SQLite rejects
                // should report which rule it broke, not a parse error.
                schema.validate()?;
                self.conn().execute_batch(&schema.to_ddl())?;
                Ok(())
            }
            Route::Ddl(sql) => {
                self.conn().execute_batch(sql)?;
                Ok(())
            }
            Route::Transform(transform) => {
                transform(self.conn_mut()).map_err(ForgeError::Transform)
            }
        }
    }

    /// Close the fixture and report what went wrong, if anything.
    ///
    /// `Drop` does the same work but cannot propagate, so a test that wants to
    /// assert clean teardown calls this instead. Afterwards `Drop` finds
    /// nothing left to do.
    pub fn close(mut self) -> Result<(), ForgeError> {
        if let Some(conn) = self.conn.take() {
            conn.close().map_err(|(_, error)| error)?;
        }
        if let Some(dir) = self.dir.take() {
            dir.close()?;
        }
        Ok(())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Order is the entire point: the connection closes here, and only then
        // does the TempDir get dropped and the directory removed.
        if let Some(conn) = self.conn.take() {
            if let Err((_, error)) = conn.close() {
                eprintln!("forger: closing fixture connection failed: {error}");
            }
        }
        if let Some(dir) = self.dir.take() {
            // Drop cannot propagate, but swallowing this silently would hide
            // exactly the Windows failure the ordering above exists to
            // prevent.
            if let Err(error) = dir.close() {
                eprintln!("forger: removing fixture directory failed: {error}");
            }
        }
    }
}

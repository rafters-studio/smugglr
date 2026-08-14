//! forger manufactures SQLite schemas and the fixtures that prove them.
//!
//! It exists because a transformation that silently changes meaning passes
//! every check written from the same premises as the transformation itself.
//! Ten defects were found by hand in smugglr's migrate spine, every one of
//! them in code with passing tests. forger's job is to be an oracle the
//! implementation's author did not write.
//!
//! # The boundary
//!
//! forger depends on no `smugglr-*` crate and never will. It does not know
//! what a transformation is -- a caller hands one in as a closure
//! ([`Route::Transform`]) and forger holds the before and after states without
//! understanding either. That is what makes it an oracle rather than a second
//! opinion from the same author, and it is what keeps extracting this crate a
//! mechanical move rather than a redesign. `scripts/check-forger-boundary.py`
//! fails CI if the rule is broken.
//!
//! # What is here
//!
//! * [`schema`] -- `Schema`/`Table`/`Column` as plain owned data, a typed
//!   builder for authoring one by hand, and the validity grammar that refuses
//!   a schema SQLite would reject.
//! * [`fixture`] -- stand a database up on either backing, hand out a
//!   connection, tear it down reliably including on panic.
//! * [`registry`] -- one [`Trait`], one schema carrying it, one seed and one
//!   probe, matched exhaustively so a trait without a probe does not compile.
//! * [`oracle`] -- the differential comparison: seed and transform one arm,
//!   build the other from scratch, hold both to the same promise and report
//!   where they part company.
//!
//! ```
//! use smugglr_forger::fixture::{Backing, Fixture, Route};
//! use smugglr_forger::schema::builder::{schema, table, Attr::*};
//! use smugglr_forger::schema::ColumnType::*;
//!
//! let target = schema()
//!     .table(
//!         table("users")
//!             .pk_int("id")
//!             .autoincrement()
//!             .col("email", Text, [NotNull, Unique, OnConflictReplace]),
//!     )
//!     .build()
//!     .expect("a valid schema");
//!
//! let mut fixture = Fixture::new(Backing::Memory).unwrap();
//! fixture.bring_to(Route::Schema(&target)).unwrap();
//! ```

pub mod error;
pub mod fixture;
pub mod oracle;
pub mod registry;
pub mod schema;

pub use error::{BoxError, ForgeError, ProbeError, ValidationError};
pub use fixture::{Backing, Fixture, Route};
pub use oracle::{differential, Arm, Divergence, Outcome, Report, TraitOutcome};
pub use registry::TraitCase;
pub use schema::{Schema, Trait};

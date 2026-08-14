//! The destructive-lint (decision 5): the load-bearing safety rail for the
//! agent-authored-migration reality.
//!
//! Before a manifest applies, every op is classified on two **independent** axes
//! -- *destructive* (loses data unless a pre-image was captured) and
//! *hash-rewriting* (rewrites content hashes, forcing the row-sync version gate).
//! The lint does two jobs over those axes:
//!
//! - [`lint_manifest`] validates each op's author-declared [`OpClass`] against the
//!   structural derive ([`Op::class`]) and **refuses an under-declared op** -- one
//!   whose declared class understates the danger on some axis. Over-declaration (a
//!   conservative author declaring an op *more* dangerous than it is) is allowed:
//!   the rail catches dishonest *understatement*, never caution.
//! - [`enforce_preimage`] gates the forward apply on what a *manifest body* can
//!   honestly be judged for before anything runs: it refuses a destructive
//!   manifest that **claims** a pre-image it does not carry. It deliberately does
//!   **not** require a pre-image payload to already exist -- see below.
//!
//! # The gate runs BEFORE capture, so it cannot check that a pre-image exists
//!
//! Pre-image capture happens *during* the forward apply, on the per-op `pre_op`
//! write-ahead hook
//! ([`PreimageCapturer::capture_before`](crate::migrate::reverse::PreimageCapturer::capture_before)),
//! which fires immediately before each op mutates. Every manifest-level gate --
//! this one included -- necessarily runs before that loop. So at gate time a
//! freshly-authored destructive manifest has no pre-image *and cannot have one*:
//! [`Manifest::preimage`] is inside the checksummed body (see
//! [`Manifest`]'s canonicalization note), so the apply that captures a pre-image
//! can never write it back into the manifest without changing that manifest's
//! checksum -- and therefore its ledger identity. The captured payload leaves the
//! apply through the driver's outcome instead (#296), never through the manifest.
//!
//! The requirement "a destructive op does not mutate without a pre-image" is
//! therefore enforced **per op, at capture time**, by `capture_before` refusing a
//! destructive op it cannot snapshot -- before the mutation, not after. Wiring
//! that capture into the loop is what
//! [`apply_with_capture`](crate::migrate::reverse::apply_with_capture) exists to
//! make structural. The order is: this gate (is the body honest?) -> per-op
//! capture (snapshot, or refuse) -> mutation.
//!
//! Scope: the lint reasons over the forward `up` path only. The reverse (`down`)
//! ops are a legitimately-destructive-by-structure inverse (a `DropColumn` undoing
//! an `AddColumn`) that needs no pre-image, and belong to reverse (#274). This
//! module provides [`enforce_preimage`] but does **not** wire it into apply -- the
//! composing driver (#296) is what calls the rail at apply time.

use crate::migrate::reverse::PreimagePayload;
use crate::migrate::{Manifest, Op, OpClass, Preimage};
use serde::Deserialize;
use thiserror::Error;

/// The two independent safety axes of a single op.
///
/// Deliberately a per-op sibling of the manifest-level [`crate::migrate::Flags`]
/// rather than a reuse of it: `Flags` is a serialized on-the-wire aggregate
/// ("*any* op loses data"), whereas this is a lint-internal, non-serialized
/// per-op verdict that carries lint-specific behaviour ([`is_additive`],
/// coverage). The two axes are **orthogonal**: an op can be both destructive and
/// hash-rewriting, which is exactly why the classification is not the single-
/// valued [`OpClass`] enum -- a single declared `OpClass` cannot honestly assert
/// both axes at once.
///
/// [`is_additive`]: Classification::is_additive
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Classification {
    /// The op loses data unless a pre-image was captured first.
    pub destructive: bool,
    /// The op rewrites content hashes (forces the row-sync version gate).
    pub hash_rewriting: bool,
}

impl Classification {
    /// The op carries neither axis: structurally reversible, no hash churn.
    pub fn is_additive(&self) -> bool {
        !self.destructive && !self.hash_rewriting
    }

    /// The axes a single declared [`OpClass`] asserts.
    ///
    /// A declared class asserts *at most one* axis, which is the crux of the
    /// under-declaration check: declaring [`OpClass::HashRewriting`] on a
    /// destructive op leaves the `destructive` axis unasserted, so it under-states
    /// the data-loss danger and is refused.
    fn asserted_by(class: OpClass) -> Self {
        match class {
            OpClass::Additive => Self {
                destructive: false,
                hash_rewriting: false,
            },
            OpClass::Destructive => Self {
                destructive: true,
                hash_rewriting: false,
            },
            OpClass::HashRewriting => Self {
                destructive: false,
                hash_rewriting: true,
            },
        }
    }

    /// True when `self` sets an axis that `declared` does not assert -- i.e. the
    /// declaration under-states the danger on at least one axis.
    ///
    /// Over-declaration (the declaration asserts an axis the derive does not) is
    /// **not** under-declaration and is allowed: the rail refuses understatement,
    /// never conservative caution.
    fn under_declared_by(&self, declared: OpClass) -> bool {
        let asserted = Self::asserted_by(declared);
        (self.destructive && !asserted.destructive)
            || (self.hash_rewriting && !asserted.hash_rewriting)
    }
}

/// Derive the honest two-axis classification of an op from its structure.
///
/// Built on [`Op::class`], the authoritative structural classifier, rather than
/// re-deriving (manifest.rs owns that logic; this must not drift from it). Note
/// that `Op::class` is single-valued and no 0.5.0 structural op is hash-rewriting
/// -- the row-transforming backfills that would set both axes arrive with the
/// apply engine -- so today `classify_op` yields at most one axis. The
/// both-axes case is reachable by constructing a [`Classification`] directly and
/// is preserved in the type for those later ops.
pub fn classify_op(op: &Op) -> Classification {
    let class = op.class();
    Classification {
        destructive: matches!(class, OpClass::Destructive),
        hash_rewriting: matches!(class, OpClass::HashRewriting),
    }
}

/// Errors from the destructive-lint.
///
/// Kept **local** to the lint (error.rs is untouched): the composing driver
/// (#296) is what bridges these into the crate error type when it wires the rail
/// into apply. Both variants carry the offending op's index into the manifest's
/// `up` list so a caller can point at the exact op.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LintError {
    /// An op's declared [`OpClass`] understates its structural danger on some
    /// axis (e.g. a `DropColumn` declared `additive`, or a destructive op
    /// declared `hash_rewriting` -- neither asserts the data-loss axis).
    #[error(
        "up[{index}]: declared op_class {declared:?} under-states the derived danger {derived:?}"
    )]
    UnderDeclared {
        /// Index into the manifest's `up` list.
        index: usize,
        /// The author-declared class.
        declared: OpClass,
        /// The honest two-axis derive.
        derived: Classification,
    },

    /// A destructive manifest carries a [`Preimage`] that holds no capture at all
    /// -- an empty `Inline` placeholder, an unparseable one, or a `Ref` with a
    /// blank key. The body claims a pre-image exists somewhere; it does not.
    ///
    /// This is the *dishonest-body* refusal, and it is the only pre-image verdict
    /// a manifest-level gate can honestly reach: see the module docs on why
    /// "the field is absent" is the **normal** state of a destructive manifest
    /// awaiting apply, not a violation.
    #[error(
        "up[{index}]: destructive op refused -- the manifest carries a pre-image that holds no \
         capture (an empty placeholder claims a snapshot that does not exist)"
    )]
    PlaceholderPreimage {
        /// Index into the manifest's `up` list.
        index: usize,
    },
}

/// Lint every forward op in a manifest, refusing any under-declared op.
///
/// On success, returns the **effective** per-op [`Classification`] of each `up`
/// op, in order -- the union of the structural derive and the author's declared
/// [`OpClass`]. Unioning is what surfaces the one hash-rewriting signal 0.5.0 has:
/// no structural op derives [`OpClass::HashRewriting`] (the row-transforming
/// backfills that would arrive with the apply engine), so the axis is reachable
/// today only by an author declaring it -- and that declaration must survive into
/// the output the driver (#296) keys on to warn on hash-rewriting cost. Honouring
/// the declared axis here is consistent with the under-declaration rule already
/// treating a declaration as a real assertion of danger.
///
/// This is a *surfacing* verdict, distinct from the *hard gate*: the returned
/// `destructive` axis also honours over-declaration (an author flagging caution),
/// whereas [`enforce_preimage`] refuses only on the **derived** destructive axis
/// -- a structurally-additive op has nothing to snapshot. The two gates are
/// independent so either can run standalone; this one does **not** check
/// pre-image capture.
pub fn lint_manifest(manifest: &Manifest) -> Result<Vec<Classification>, LintError> {
    let mut classifications = Vec::with_capacity(manifest.up.len());
    for (index, classified) in manifest.up.iter().enumerate() {
        let derived = classify_op(&classified.op);
        if derived.under_declared_by(classified.op_class) {
            return Err(LintError::UnderDeclared {
                index,
                declared: classified.op_class,
                derived,
            });
        }
        let asserted = Classification::asserted_by(classified.op_class);
        classifications.push(Classification {
            destructive: derived.destructive || asserted.destructive,
            hash_rewriting: derived.hash_rewriting || asserted.hash_rewriting,
        });
    }
    Ok(classifications)
}

/// Refuse the forward apply of a destructive manifest whose carried pre-image is
/// a placeholder holding no capture.
///
/// # Ordering: this runs before capture, and is scoped to what that allows
///
/// This is the pre-apply gate, so it runs **before** the apply loop in which
/// pre-images are captured (module docs, "The gate runs BEFORE capture"). Two
/// consequences define its whole contract:
///
/// - `manifest.preimage == None` on a destructive manifest is **not** a refusal.
///   It is the expected state of every honestly-authored destructive migration
///   awaiting its first apply: nothing has captured yet, and nothing *can* write
///   a capture into the checksummed body afterwards. Refusing it made the honest
///   path unrunnable while stopping nothing (#326).
/// - `manifest.preimage == Some(placeholder)` **is** a refusal. A body that
///   asserts a pre-image while carrying no capture is a lie, and it is exactly the
///   bypass an author reached for while the absent-field refusal was in force. A
///   [`Preimage::Inline`] whose payload holds no [`crate::migrate::reverse::TablePreimage`]
///   (`{"tables": []}`, `null`, anything that will not deserialize) and a
///   [`Preimage::Ref`] with a blank key all fail here. Note that a *present*
///   capture with zero captured rows is legitimate -- dropping a column of an
///   empty table loses no rows -- so the test is "is there a capture", never
///   "are there rows".
///
/// The guarantee that a destructive op does not mutate without a snapshot is
/// enforced per-op at capture time by
/// [`PreimageCapturer::capture_before`](crate::migrate::reverse::PreimageCapturer::capture_before),
/// which refuses before the mutation; wiring that hook into the loop is what
/// [`apply_with_capture`](crate::migrate::reverse::apply_with_capture) makes
/// structural.
///
/// Gates on the **derived** classification ([`classify_op`]), not the declared
/// class, for the same reason [`lint_manifest`]'s surfacing verdict does not: a
/// structurally-additive op has nothing to snapshot, so keying on a (possibly
/// over-declared) destructive *declaration* would refuse a manifest that risks no
/// data. A drop dishonestly declared additive is still judged here, because the
/// derive sees the data loss -- so this gate holds even when [`lint_manifest`] was
/// not run first.
pub fn enforce_preimage(manifest: &Manifest) -> Result<(), LintError> {
    let Some(preimage) = manifest.preimage.as_ref() else {
        return Ok(());
    };
    if !is_placeholder(preimage) {
        return Ok(());
    }
    for (index, classified) in manifest.up.iter().enumerate() {
        if classify_op(&classified.op).destructive {
            return Err(LintError::PlaceholderPreimage { index });
        }
    }
    Ok(())
}

/// Whether a carried [`Preimage`] holds no capture at all.
///
/// Deserializes the inline payload into the real
/// [`PreimagePayload`](crate::migrate::reverse::PreimagePayload) rather than
/// poking at JSON keys, so this cannot drift from the shape reverse (#274)
/// actually writes; a payload that will not deserialize into one carries no
/// capture either, so it is a placeholder too. Borrows the `Value` (serde
/// deserializes from `&Value`), so a large inline payload is not cloned to answer
/// the question.
fn is_placeholder(preimage: &Preimage) -> bool {
    match preimage {
        Preimage::Inline { rows } => PreimagePayload::deserialize(rows)
            .map(|payload| payload.is_empty())
            .unwrap_or(true),
        Preimage::Ref { key } => key.trim().is_empty(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrate::reverse::{CapturedValue, TablePreimage};
    use crate::migrate::{ClassifiedOp, Column, ColumnKind, Flags, Manifest, Op, Preimage};

    /// A minimal typed column with no constraints or tags.
    fn col(name: &str, kind: ColumnKind) -> Column {
        Column {
            name: name.to_string(),
            kind,
            constraints: Vec::new(),
            tags: Vec::new(),
        }
    }

    fn add_column() -> Op {
        Op::AddColumn {
            table: "users".into(),
            column: col("email", ColumnKind::Text),
        }
    }

    fn drop_column() -> Op {
        Op::DropColumn {
            table: "users".into(),
            column: "email".into(),
        }
    }

    /// A manifest carrying exactly the given forward ops, no pre-image.
    fn manifest_with(up: Vec<ClassifiedOp>) -> Manifest {
        Manifest {
            version: 1,
            target_schema: "opaque".into(),
            up,
            down: Vec::new(),
            preimage: None,
            flags: Flags::default(),
            author: None,
        }
    }

    // --- classify_op pins every Op variant ---------------------------------

    #[test]
    fn classify_op_pins_every_variant() {
        let additive = Classification {
            destructive: false,
            hash_rewriting: false,
        };
        let destructive = Classification {
            destructive: true,
            hash_rewriting: false,
        };

        assert_eq!(
            classify_op(&Op::CreateTable {
                table: "t".into(),
                columns: vec![col("id", ColumnKind::Int)],
                without_rowid: false,
            }),
            additive
        );
        assert_eq!(classify_op(&add_column()), additive);
        assert_eq!(
            classify_op(&Op::CreateIndex {
                name: "i".into(),
                table: "t".into(),
                columns: vec!["c".into()],
                unique: false,
            }),
            additive
        );
        assert_eq!(classify_op(&Op::DropIndex { name: "i".into() }), additive);
        assert_eq!(
            classify_op(&Op::RenameTable {
                from: "a".into(),
                to: "b".into(),
            }),
            additive
        );
        assert_eq!(
            classify_op(&Op::RenameColumn {
                table: "t".into(),
                from: "a".into(),
                to: "b".into(),
            }),
            additive
        );

        assert_eq!(
            classify_op(&Op::DropTable { table: "t".into() }),
            destructive
        );
        assert_eq!(classify_op(&drop_column()), destructive);
    }

    #[test]
    fn additive_ops_are_additive() {
        assert!(classify_op(&add_column()).is_additive());
    }

    #[test]
    fn destructive_ops_are_not_additive() {
        let c = classify_op(&drop_column());
        assert!(!c.is_additive());
        assert!(c.destructive);
        assert!(!c.hash_rewriting);
    }

    // --- the two-axis model: destructive AND hash-rewriting ----------------

    #[test]
    fn a_both_axis_op_is_under_declared_by_either_single_class() {
        // The capability the two-axis model exists for: an op that is both
        // destructive and hash-rewriting. No single declared OpClass asserts both
        // axes, so *every* single-valued declaration under-states it -- including
        // the two that each cover one axis. This is why the classification is not
        // the single OpClass enum.
        let both = Classification {
            destructive: true,
            hash_rewriting: true,
        };
        assert!(both.under_declared_by(OpClass::Additive));
        assert!(both.under_declared_by(OpClass::Destructive));
        assert!(both.under_declared_by(OpClass::HashRewriting));
    }

    #[test]
    fn hash_rewriting_declaration_does_not_cover_the_destructive_axis() {
        // The orthogonality proof: HashRewriting is *not* "more severe than"
        // Destructive on a linear scale -- it simply asserts a different axis, so
        // it under-states a destructive op.
        let destructive = classify_op(&drop_column());
        assert!(destructive.under_declared_by(OpClass::HashRewriting));
    }

    // --- lint_manifest: honest, over-declared, under-declared ---------------

    #[test]
    fn lint_accepts_an_honest_manifest() {
        let m = manifest_with(vec![
            ClassifiedOp::new(add_column()),
            ClassifiedOp::new(drop_column()),
        ]);
        let classes = lint_manifest(&m).expect("honest manifest lints clean");
        assert_eq!(classes.len(), 2);
        assert!(classes[0].is_additive());
        assert!(classes[1].destructive);
    }

    #[test]
    fn lint_allows_over_declaration() {
        // A conservative author declaring an additive op HashRewriting is caution,
        // not danger -- the rail must not refuse it.
        let m = manifest_with(vec![ClassifiedOp::declared(
            add_column(),
            OpClass::HashRewriting,
        )]);
        assert!(lint_manifest(&m).is_ok());
    }

    #[test]
    fn lint_surfaces_author_declared_hash_rewriting() {
        // The only hash-rewriting signal 0.5.0 has is an author declaration (no
        // structural op derives it). lint_manifest must carry that axis into its
        // output so the driver (#296) can warn on cost -- returning derived-only
        // would silently drop it.
        let m = manifest_with(vec![ClassifiedOp::declared(
            add_column(),
            OpClass::HashRewriting,
        )]);
        let classes = lint_manifest(&m).expect("over-declaration lints clean");
        assert!(classes[0].hash_rewriting);
        assert!(!classes[0].destructive);
    }

    #[test]
    fn lint_refuses_a_drop_declared_additive() {
        let m = manifest_with(vec![ClassifiedOp::declared(
            drop_column(),
            OpClass::Additive,
        )]);
        let err = lint_manifest(&m).expect_err("under-declared drop must refuse");
        match err {
            LintError::UnderDeclared {
                index,
                declared,
                derived,
            } => {
                assert_eq!(index, 0);
                assert_eq!(declared, OpClass::Additive);
                assert!(derived.destructive);
            }
            other => panic!("expected UnderDeclared, got {other:?}"),
        }
    }

    #[test]
    fn lint_refuses_a_destructive_op_declared_hash_rewriting() {
        // The two-axis refusal at the manifest level: HashRewriting leaves the
        // destructive axis unasserted, so a drop declared HashRewriting is refused.
        let m = manifest_with(vec![ClassifiedOp::declared(
            drop_column(),
            OpClass::HashRewriting,
        )]);
        let err = lint_manifest(&m).expect_err("hash_rewriting on a drop must refuse");
        assert!(matches!(err, LintError::UnderDeclared { index: 0, .. }));
    }

    #[test]
    fn lint_reports_the_index_of_the_offending_op() {
        let m = manifest_with(vec![
            ClassifiedOp::new(add_column()),
            ClassifiedOp::new(add_column()),
            ClassifiedOp::declared(drop_column(), OpClass::Additive),
        ]);
        let err = lint_manifest(&m).expect_err("under-declared op at index 2");
        assert!(matches!(err, LintError::UnderDeclared { index: 2, .. }));
    }

    // --- enforce_preimage ---------------------------------------------------

    /// A one-table pre-image that actually carries a capture, serialized the way
    /// reverse (#274) writes it.
    fn real_inline_preimage() -> Preimage {
        let payload = PreimagePayload {
            tables: vec![TablePreimage::Column {
                table: "users".into(),
                dropped: "email".into(),
                create_sql: "CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT)".into(),
                aux_ddl: Vec::new(),
                dropped_requires_value: false,
                pk: vec!["id".into()],
                captured_columns: vec!["id".into(), "email".into()],
                rows: vec![vec![
                    CapturedValue::Text("u1".into()),
                    CapturedValue::Text("a@x".into()),
                ]],
            }],
        };
        Preimage::Inline {
            rows: serde_json::to_value(&payload).expect("payload serializes"),
        }
    }

    /// The placeholder that used to clear the old presence check: a well-formed
    /// `Inline` whose payload holds no capture at all.
    fn empty_inline() -> Preimage {
        Preimage::Inline {
            rows: serde_json::json!({ "tables": [] }),
        }
    }

    #[test]
    fn enforce_allows_additive_only_without_preimage() {
        let m = manifest_with(vec![ClassifiedOp::new(add_column())]);
        assert!(enforce_preimage(&m).is_ok());
    }

    #[test]
    fn enforce_allows_a_generator_shaped_destructive_manifest_with_no_preimage_yet() {
        // The #326 headline: an honestly-authored destructive manifest -- derived
        // op classes, `preimage: None` -- must pass the pre-apply gate, because the
        // pre-image is captured *inside* the apply this gate runs before. Refusing
        // here made the honest path unrunnable.
        let m = manifest_with(vec![ClassifiedOp::new(drop_column())]);
        assert!(m.preimage.is_none());
        assert!(enforce_preimage(&m).is_ok());
    }

    #[test]
    fn enforce_allows_destructive_with_a_real_preimage() {
        let mut m = manifest_with(vec![ClassifiedOp::new(drop_column())]);
        m.preimage = Some(Preimage::Ref {
            key: "snapshot-key".into(),
        });
        assert!(enforce_preimage(&m).is_ok());

        m.preimage = Some(real_inline_preimage());
        assert!(enforce_preimage(&m).is_ok());
    }

    #[test]
    fn enforce_refuses_an_empty_inline_placeholder() {
        // The bypass #296's own test used: an `Inline` carrying nothing at all
        // cleared the old presence check. It is a claim of a snapshot that does
        // not exist, so it is now the thing that refuses.
        let mut m = manifest_with(vec![ClassifiedOp::new(drop_column())]);
        m.preimage = Some(empty_inline());
        let err = enforce_preimage(&m).expect_err("an empty Inline carries no capture");
        assert!(matches!(err, LintError::PlaceholderPreimage { index: 0 }));
    }

    #[test]
    fn enforce_refuses_every_shape_of_empty_preimage() {
        // Null, a bare object, a payload that will not deserialize into a
        // PreimagePayload at all, and a blank relay key: none of them carries a
        // capture, so none of them may pass.
        for rows in [
            serde_json::Value::Null,
            serde_json::json!({}),
            serde_json::json!("snapshot"),
            serde_json::json!({ "tables": "not-a-list" }),
        ] {
            let mut m = manifest_with(vec![ClassifiedOp::new(drop_column())]);
            m.preimage = Some(Preimage::Inline { rows: rows.clone() });
            assert!(
                matches!(
                    enforce_preimage(&m),
                    Err(LintError::PlaceholderPreimage { index: 0 })
                ),
                "inline {rows} must refuse"
            );
        }

        for key in ["", "   "] {
            let mut m = manifest_with(vec![ClassifiedOp::new(drop_column())]);
            m.preimage = Some(Preimage::Ref { key: key.into() });
            assert!(
                matches!(
                    enforce_preimage(&m),
                    Err(LintError::PlaceholderPreimage { index: 0 })
                ),
                "blank ref key {key:?} must refuse"
            );
        }
    }

    #[test]
    fn enforce_ignores_a_placeholder_on_an_additive_only_manifest() {
        // The gate is scoped to destructive ops: a pointless placeholder on a
        // manifest that loses no data risks nothing.
        let mut m = manifest_with(vec![ClassifiedOp::new(add_column())]);
        m.preimage = Some(empty_inline());
        assert!(enforce_preimage(&m).is_ok());
    }

    #[test]
    fn enforce_catches_a_drop_dishonestly_declared_additive() {
        // enforce_preimage keys on the derived class, so it holds even when the
        // declaration lies and lint_manifest was not run first.
        let mut m = manifest_with(vec![ClassifiedOp::declared(
            drop_column(),
            OpClass::Additive,
        )]);
        m.preimage = Some(empty_inline());
        let err = enforce_preimage(&m).expect_err("a derived-destructive drop is still judged");
        assert!(matches!(err, LintError::PlaceholderPreimage { index: 0 }));
    }

    #[test]
    fn enforce_reports_the_first_destructive_index() {
        let mut m = manifest_with(vec![
            ClassifiedOp::new(add_column()),
            ClassifiedOp::new(drop_column()),
        ]);
        m.preimage = Some(empty_inline());
        let err = enforce_preimage(&m).expect_err("destructive op at index 1");
        assert!(matches!(err, LintError::PlaceholderPreimage { index: 1 }));
    }
}

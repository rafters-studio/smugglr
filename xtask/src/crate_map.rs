//! Holds the workspace to the crate map (NFR-SMUGGLR-009).
//!
//! The map says which workspace crates each crate may depend on. A crate the
//! map names that does not exist yet is held to its row when it arrives; a
//! workspace crate the map does not name may depend on no workspace crate, and
//! no mapped crate may depend on it. smugglr-core is pure: besides depending on
//! no workspace crate, it depends only on external crates listed in
//! `CORE_MAY_USE`, none of which does I/O or reads a clock.

use std::collections::BTreeSet;
use std::fmt;

use cargo_metadata::{DependencyKind, Metadata};

const CORE: &str = "smugglr-core";

/// Each crate the map names, and the workspace crates it may depend on.
/// Current choice 2026-10-19 (NFR-SMUGGLR-009 rev 5); revisable.
const CRATE_MAP: &[(&str, &[&str])] = &[
    (CORE, &[]),
    ("smugglr-memory", &[CORE]),
    ("smugglr-place", &[CORE]),
    ("smugglr-multi", &[CORE, "smugglr-memory", "smugglr-place"]),
    (
        "smugglr-mesh",
        &[CORE, "smugglr-memory", "smugglr-place", "smugglr-multi"],
    ),
    (
        "smugglr",
        &[
            CORE,
            "smugglr-memory",
            "smugglr-place",
            "smugglr-multi",
            "smugglr-mesh",
        ],
    ),
    ("smugglr-web", &[CORE, "smugglr-memory"]),
    ("xtask", &[]),
];

/// External crates smugglr-core may depend on. Each one does no I/O and reads
/// no clock; a crate joins this list only after that is checked.
const CORE_MAY_USE: &[&str] = &[];

/// One way the workspace leaves the crate map.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Violation {
    /// `from` depends on the workspace crate `to`, and the map does not allow it.
    Edge {
        /// The crate that has the dependency.
        from: String,
        /// The workspace crate it depends on.
        to: String,
    },
    /// smugglr-core depends on an external crate not known to be free of I/O
    /// and clocks.
    Impure {
        /// The external crate smugglr-core depends on.
        dependency: String,
    },
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Edge { from, to } => {
                write!(
                    f,
                    "{from} depends on {to}, which the crate map does not allow"
                )
            }
            Self::Impure { dependency } => write!(
                f,
                "{CORE} depends on {dependency}, which is not known to be free of I/O and clocks"
            ),
        }
    }
}

/// Checks the workspace that `metadata` describes against the crate map and
/// returns every violation, sorted; an empty list means the map holds.
///
/// Every dependency kind counts as an edge between workspace crates. For
/// smugglr-core's external crates, normal and build dependencies count;
/// dev-dependencies never reach a build of the crate itself.
#[must_use]
pub fn check(metadata: &Metadata) -> Vec<Violation> {
    let members: BTreeSet<&str> = metadata
        .workspace_packages()
        .iter()
        .map(|package| package.name.as_str())
        .collect();
    let mut violations = BTreeSet::new();
    for package in metadata.workspace_packages() {
        let from = package.name.as_str();
        let allowed = CRATE_MAP
            .iter()
            .find(|(name, _)| *name == from)
            .map_or(&[][..], |(_, allowed)| *allowed);
        for dependency in &package.dependencies {
            let to = dependency.name.as_str();
            if members.contains(to) {
                if !allowed.contains(&to) {
                    violations.insert(Violation::Edge {
                        from: from.to_owned(),
                        to: to.to_owned(),
                    });
                }
            } else if from == CORE
                && dependency.kind != DependencyKind::Development
                && !CORE_MAY_USE.contains(&to)
            {
                violations.insert(Violation::Impure {
                    dependency: to.to_owned(),
                });
            }
        }
    }
    violations.into_iter().collect()
}

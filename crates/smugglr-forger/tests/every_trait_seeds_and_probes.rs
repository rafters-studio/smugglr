//! Every trait's case stands up, seeds, and passes its own probe -- and every
//! probe refuses a database nobody seeded.
//!
//! The second half is the one that matters. An empty database is green on every
//! behavioural assertion ever written, because there are no rows for the
//! behaviour to happen to; a probe that passes there is measuring nothing and
//! would go on measuring nothing after the construct it guards was lost.

use smugglr_forger::error::ProbeError;
use smugglr_forger::fixture::{Backing, Fixture, Route};
use smugglr_forger::registry::TraitCase;
use smugglr_forger::schema::Trait;

fn stood_up(case: &TraitCase) -> Fixture {
    let mut fixture = Fixture::new(Backing::Memory).expect("fixture");
    fixture
        .bring_to(Route::Schema(&case.schema))
        .unwrap_or_else(|error| panic!("{:?}: {error}", case.kind));
    fixture
}

#[test]
fn every_trait_passes_its_own_probe_once_seeded() {
    for kind in Trait::ALL {
        let case = TraitCase::for_trait(kind);
        let fixture = stood_up(&case);
        case.seed(fixture.conn())
            .unwrap_or_else(|error| panic!("{kind:?} seed: {error}"));
        case.probe(fixture.conn())
            .unwrap_or_else(|error| panic!("{kind:?} probe: {error}"));
    }
}

#[test]
fn every_probe_refuses_an_unseeded_database() {
    for kind in Trait::ALL {
        let case = TraitCase::for_trait(kind);
        let fixture = stood_up(&case);

        // The schema is in place and correct. Only the rows are missing, which
        // is exactly the state in which a badly written probe reports health.
        let outcome = case.probe(fixture.conn());
        assert!(
            matches!(outcome, Err(ProbeError::Unseeded(_))),
            "{kind:?} probed an unseeded database and reported {outcome:?}"
        );
    }
}

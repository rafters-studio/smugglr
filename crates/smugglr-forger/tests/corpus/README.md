# The regression corpus

Every `.json` file in this directory is a failure somebody found, written down.
`tests/corpus_runs.rs` walks the directory on every `cargo test`, stands each
one back up, and requires the probe to report the same failure it reported the
day it was pinned. Adding a fixture is dropping a file in here. There is no
list to append to and no code to change, because the moment a fixture is worth
committing is the moment somebody is mid-debug and has the least patience for
either.

Anything that is not a `.json` file -- this README, an editor's leavings -- is
skipped. A `.json` file that does not parse is a failure, never a skip.

## The fields

| field | |
| --- | --- |
| `provenance` | What this is, in the words of whoever pinned it: the issue it reproduces, the run it was shrunk from, the defect shape it stands for. Required, and refused empty. It is a field rather than a comment so that it survives being rewritten and can be read by something other than a person. |
| `trait` | A `Trait` variant. It selects the registry case that supplies the seed, the probe, and the unbroken schema the probe is handed as the promise. |
| `schema` | The schema as the transformation left it. Stood up as rendered DDL and deliberately not validated -- what is recorded is what something *produced*, and a transformation that mangled a schema is under no obligation to have produced one forger's own grammar accepts. |
| `after_seed` | Optional. Statements run after the seed, for defects that live in what a rebuild *did* rather than in what it declared. `336-a-rebuild-refired-a-trigger-over-copied-rows.json` is the shape: its schema is correct in every particular and its database audited the same row twice. |
| `expected_failure` | The probe's message, verbatim. Matched by equality. |

Unknown keys are refused. A file is hand-editable, and `after_seeds` silently
ignored would leave one that looks like it runs statements after the seed and
does not.

## Writing one

Do not type a fixture from scratch. Build the failing `Schema` in Rust, run it,
take the message the probe actually reported, and serialize the whole thing:

```rust
let regression = Regression {
    provenance: "smugglr#NNN. What went wrong, and where this came from.".into(),
    kind: Trait::ForeignKeyWithAction,
    schema: the_schema_that_failed,
    after_seed: Vec::new(),
    expected_failure: what_the_probe_said,
};
std::fs::write(path, regression.to_json())?;
```

`expected_failure` is matched exactly, which means that when FR-FORGER-008
reformats what a probe says, every fixture recording that message has to be
re-recorded. That is a real cost and it is the one worth paying: a substring
match admits the empty string, and a fixture that matches every failure is
green on a defect it was never about.

## When a fixture stops failing

The runner reports it, and the fixture is not simply deleted. Either the defect
was fixed -- in which case say so in the commit and remove the fixture, since a
guard against something that cannot happen is runtime nobody is buying anything
with -- or the fixture has drifted off the thing it was about, which is a
different problem and a worse one.

## Size and runtime

The runner prints the count and the total on every run. A corpus that only
grows becomes a slow suite people skip locally and reviewers stop reading. The
pruning policy is an open question and nothing here settles it; the number
being visible before it bites is the minimum.

---
name: Implementation Task
about: Implementation task for AI agents with full dev workflow
title: "feat: [brief description]"
labels: enhancement
assignees: ''
---

## Goal

**Single, focused objective this task achieves.**

## Traces to

**Which requirement does this satisfy?** Check before writing anything else:

```
legion document list --owner <repo> --doc-type requirement
legion document view <FR-ID>
```

- FR-XXXX-NNN -- what this issue does about it
- FR-XXXX-NNN [criteria: 1, 3] -- name the criterion ids when only some are in scope

The first token of the bullet is the document id and the rest is for humans, so
do not wrap the id in backticks. Omitting the criteria bracket means the whole
requirement is in scope. Several FR bullets are fine.

If nothing covers this:

- None -- the reason, in one line

The reason is required, and None cannot sit alongside an FR id. Pick one.

**Where a requirement exists, this issue's acceptance criteria come FROM it.**
Writing parallel ones is how the spec stops being the source and the issues
become the source. It is worse than leaving the trace off, because an issue
citing the wrong requirement while inventing a conflicting criterion passes
every gate -- that is smugglr#411, which shipped a hand-written boundary line
against FR-FORGER-010's requirement that the boundary be derived.

A CANCELLED requirement is not a trace. Check the status before citing it.

## Requirements

### Interface
```rust
// Exact struct definitions, trait signatures, or API expected
```

### Behavior
- Specific requirement 1 with clear success criteria
- Specific requirement 2 with measurable outcome
- Specific requirement 3 with validation method

### Error Handling
- What errors to return and when
- Required error types and messages

## Out of Scope

- Feature 1 (separate issue)
- Feature 2 (future consideration)

## File Locations

- Implementation: `crates/smugglr-core/src/module_name.rs`
- Tests: Bottom of same file in `#[cfg(test)]` module
- CLI integration: `crates/smugglr/src/main.rs`

## Dev Workflow

Each step is mandatory. Do not skip steps or combine them.

1. **Build** -- Implement the feature. Write tests alongside code. Run `cargo test --workspace`, `cargo clippy --workspace -- -D warnings`, `cargo fmt -- --check`. All must pass.
2. **Simplify** -- Run `/legion:legion-simplify` on all changed files. Accept structural improvements, flatten unnecessary abstractions, remove dead code. This records the `legion-simplify` gate that `legion pr create` requires; the harness `/simplify` skill is a different tool and records no gate.
3. **Review** -- Run `/legion:legion-review`, which fans out parallel review dimensions (spec-vs-diff, correctness, quality, security) and adversarially verifies each finding. Do not create the PR yet.
4. **Fix** -- Address every issue the review found. Re-run tests after fixes.
5. **PR** -- Create the PR. Reference this issue number.

### Rust Rules
- No `unwrap()` in production code
- No `unsafe` code
- No emoji in code, comments, or documentation
- `cargo clippy --workspace -- -D warnings` must pass
- `cargo fmt -- --check` must pass
- Errors use thiserror derive macros

## Done When

- [ ] All tests pass
- [ ] Simplify pass completed
- [ ] Review pass completed and issues fixed
- [ ] PR created and linked to this issue

**This issue is complete when:** [Specific, measurable completion condition -- restating the traced requirement's acceptance criterion, not a new one]

## Context

- Related issues: #N, #M
- Design docs: link if applicable

# Releasing smugglr

Pushing a tag `vX.Y.Z` runs `.github/workflows/release.yml`. It builds the
`smugglr` binary for each target onto a draft GitHub release, then publishes
the eight crates to crates.io in dependency order:

smugglr-core, smugglr-place, smugglr-memory, smugglr-sync, smugglr-multi,
smugglr-mesh, smugglr-web, smugglr.

It refuses a tag that does not match the version in `[workspace.package]` of
`Cargo.toml`, skips a crate whose version is already on crates.io, and
authenticates with crates.io trusted publishing.

## First publish: the operator's

crates.io trusted publishing can only be configured for a crate that already
exists, so the first publish of each crate is done by the operator with
`cargo publish`, not by `release.yml`. smugglr and smugglr-core are on
crates.io; these six are not:

- smugglr-place
- smugglr-memory
- smugglr-sync
- smugglr-multi
- smugglr-mesh
- smugglr-web

Logged in with `cargo login`, publish them in this order, after smugglr-core
at the same version is on crates.io:

```bash
cargo publish -p smugglr-place
cargo publish -p smugglr-memory
cargo publish -p smugglr-sync
cargo publish -p smugglr-multi
cargo publish -p smugglr-mesh
cargo publish -p smugglr-web
```

Then, for each of the eight crates on crates.io, add a trusted publisher:
repository `rafters-studio/smugglr`, workflow `release.yml`.

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

The first release this covers is 0.6.4. 0.6.3 cannot be published by this
procedure: its tag was pushed before `release.yml` had a publish job.

For 0.6.4, after the release PR merges and before its tag is pushed:

1. Check out that release commit cleanly, so what is published matches the
   tag:

   ```bash
   git fetch origin
   git checkout --detach <release commit>
   git status --porcelain    # empty
   ```

2. Logged in with `cargo login`, publish all eight in dependency order. The
   six new crates need smugglr-core at the same version on crates.io, so
   smugglr-core goes first:

   ```bash
   cargo publish -p smugglr-core
   cargo publish -p smugglr-place
   cargo publish -p smugglr-memory
   cargo publish -p smugglr-sync
   cargo publish -p smugglr-multi
   cargo publish -p smugglr-mesh
   cargo publish -p smugglr-web
   cargo publish -p smugglr
   ```

3. For each of the eight crates on crates.io, add a trusted publisher:
   repository `rafters-studio/smugglr`, workflow `release.yml`.

4. Push the tag on that same commit:

   ```bash
   git tag -s v0.6.4 -m "smugglr 0.6.4" <release commit>
   git push origin v0.6.4
   ```

   The release run builds the binaries, finds all eight crates already
   published, and skips them.

Every later release needs no operator step: pushing its tag publishes.

## A run that fails partway

If a release run fails partway through publishing, re-run it:

```bash
gh run rerun <run id> --failed
```

It publishes only the crates whose version is not yet on crates.io.

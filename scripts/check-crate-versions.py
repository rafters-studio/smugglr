#!/usr/bin/env python3
"""Assert every inter-crate dependency req equals the workspace version.

Workspace crates depend on each other with both a path and a version:

    smugglr-core = { path = "../smugglr-core", version = "0.4.2" }

Local builds and CI resolve those deps by PATH and never read the version
string, so it can rot for months with every check green. `cargo publish`
strips the path and ships the registry req -- it is the first and only
reader, and by then the artifact is immutable.

The failure is silent rather than loud. A req naming a version that does
not exist would fail resolution and stop the release. A req naming a
version that DOES exist resolves fine -- to the wrong crate. Publishing
smugglr 0.5.0 while its req still reads "0.4.2" ships a 0.5.0 CLI hard-
depending on a four-versions-stale engine, with a green pipeline start to
finish.

So: every inter-crate req moves in the same commit as the workspace
version bump. That is the invariant this script enforces.

Reads `cargo metadata` rather than parsing manifests, so it sees the same
normalized reqs cargo itself resolves ("0.5.0" -> "^0.5.0").
"""

from __future__ import annotations

import json
import subprocess
import sys

# Deps whose reqs must track the workspace version. Third-party deps are
# none of this script's business.
INTERNAL_PREFIX = "smugglr"


def workspace_version(meta: dict) -> str:
    """The single version every publishable workspace member shares.

    Publishable members inherit `version.workspace = true`, so any of
    their versions is the workspace version -- but assert that rather
    than trusting it, because one that opts out is exactly the kind of
    drift this script exists to catch.

    `publish = false` members are excluded from the consensus, not
    overlooked by it. Their versions never reach the registry and no req
    is ever resolved against them, so a crate like smugglr-forger -- an
    independent artifact that happens to live here, sitting at 0.1.0
    while the workspace is at 0.5.x -- is not drift. It is the point.
    """
    members = {
        p["name"]: p["version"] for p in meta["packages"] if p.get("publish") != []
    }
    versions = set(members.values())
    if len(versions) != 1:
        detail = ", ".join(f"{n} {v}" for n, v in sorted(members.items()))
        raise SystemExit(
            "workspace members do not share one version, so there is no "
            f"single version to check reqs against: {detail}"
        )
    return versions.pop()


def main() -> int:
    proc = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        print(proc.stderr, file=sys.stderr)
        raise SystemExit("cargo metadata failed")

    meta = json.loads(proc.stdout)
    expected_version = workspace_version(meta)
    expected_req = f"^{expected_version}"

    failures: list[str] = []
    checked = 0

    for pkg in sorted(meta["packages"], key=lambda p: p["name"]):
        # `publish = false` crates never reach the registry, so their reqs
        # are never read by anyone. cargo metadata reports the field as []
        # when publishing is disabled and null when it is allowed.
        if pkg.get("publish") == []:
            continue

        for dep in pkg["dependencies"]:
            if not dep["name"].startswith(INTERNAL_PREFIX):
                continue

            # A path-only dep ships no version req at all, which means
            # `cargo publish` cannot express the dependency and refuses.
            # Report it here rather than at publish time.
            if dep["req"] == "*":
                failures.append(
                    f"{pkg['name']} -> {dep['name']}: no version req "
                    f"(path-only deps are unpublishable; add version = "
                    f'"{expected_version}")'
                )
                continue

            checked += 1
            if dep["req"] != expected_req:
                failures.append(
                    f"{pkg['name']} -> {dep['name']}: req {dep['req']} "
                    f"!= workspace version {expected_version}"
                )

    if failures:
        print(
            f"inter-crate version reqs disagree with the workspace version "
            f"({expected_version}):\n",
            file=sys.stderr,
        )
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        print(
            "\nThese reqs are what `cargo publish` ships once the path is "
            "stripped. Local builds resolve by path and cannot catch this.\n"
            "Bump every inter-crate req in the same commit as "
            "[workspace.package].version.",
            file=sys.stderr,
        )
        return 1

    print(f"ok: {checked} inter-crate reqs all pin {expected_version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

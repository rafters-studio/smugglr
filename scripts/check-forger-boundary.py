#!/usr/bin/env python3
"""Assert smugglr-forger depends on no smugglr crate, and nothing depends on
it outside test builds.

forger manufactures schemas and fixtures to test a transformation it knows
nothing about. That ignorance is the whole value: an oracle written from the
same premises as the implementation reproduces the implementation's blind
spots, which is how ten defects reached a migrate spine whose tests were
green.

The rule is not the crate name. A crate that compiles standalone can be
renamed at publish time in an afternoon; a crate that reached into
smugglr-core cannot be extracted at any name. So the boundary is the
dependency direction, and it runs exactly one way:

    smugglr's tests --> forger          (dev-dependencies only)
    forger --> anything smugglr         (never)

Both halves are checked here. The forward rule bites today. The reverse rule
is vacuous until smugglr's tests start using forger, and binding from the
moment they do -- a normal dependency on forger would put forger in the
shipped artifact and, together with the forward rule, form a cycle.

Every dependency kind counts, dev and build included. A dev-dependency on
smugglr-core would still break "forger builds with the smugglr crates absent
from the workspace", which is the property that makes extraction mechanical.

Reads `cargo metadata` rather than parsing manifests, so a dependency added
through any spelling -- path, version, workspace inheritance, a target-
specific table -- is seen.
"""

from __future__ import annotations

import json
import subprocess
import sys

FORGER = "smugglr-forger"
INTERNAL_PREFIX = "smugglr"


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
    packages = {p["name"]: p for p in meta["packages"]}

    forger = packages.get(FORGER)
    if forger is None:
        # The crate is the subject of the check. If it is gone, the check is
        # not passing, it is not running.
        raise SystemExit(f"{FORGER} is not a member of this workspace")

    failures: list[str] = []

    # Forward: forger reaches for nothing of ours.
    for dep in forger["dependencies"]:
        if dep["name"].startswith(INTERNAL_PREFIX):
            kind = dep.get("kind") or "normal"
            failures.append(
                f"{FORGER} declares a {kind} dependency on {dep['name']}. "
                f"forger must know nothing about smugglr -- a transformation "
                f"is handed in as a closure, never imported."
            )

    # Reverse: whoever uses forger uses it in test builds only.
    for name, pkg in sorted(packages.items()):
        if name == FORGER:
            continue
        for dep in pkg["dependencies"]:
            if dep["name"] != FORGER:
                continue
            # cargo metadata reports kind as null for a normal dependency,
            # "dev" for dev-dependencies and "build" for build-dependencies.
            if dep.get("kind") != "dev":
                kind = dep.get("kind") or "normal"
                failures.append(
                    f"{name} declares a {kind} dependency on {FORGER}. "
                    f"forger is test-only scaffolding: a non-dev dependency "
                    f"ships it to users and closes the loop the forward rule "
                    f"opens."
                )

    if failures:
        print("the forger dependency boundary is broken:\n", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        print(
            "\nThis boundary is what keeps extracting forger a mechanical "
            "move rather than a redesign. See FR-FORGER-011.",
            file=sys.stderr,
        )
        return 1

    print(f"ok: {FORGER} depends on no smugglr crate, and nothing depends on it outside tests")
    return 0


if __name__ == "__main__":
    sys.exit(main())

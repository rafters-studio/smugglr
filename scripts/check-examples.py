#!/usr/bin/env python3
"""Run the credential-free CLI examples and compare their output to their READMEs.

Every fenced block in an example README that starts with `$ smugglr ...` or
`$ sqlite3 ...` is a claim: run this, see that. This script re-runs each
example's setup and every such block in a scratch copy of the example
directory and checks the claim.

A block is a fence whose first line starts with `$ `; the command and its
output share one fence, as every checked README writes them. A separate
command fence followed by a bare output fence is not read, and an example
whose README yields no readable block fails rather than passing empty.

Every command must exit 0; the READMEs show no failing commands.

Comparison is deliberately loose in two ways the binary forces. The text
summary lists tables in map order, so lines are compared as sorted sets.
JSON objects are parsed and their `tables` arrays sorted by name before
comparison. Everything else must match exactly.

Usage: scripts/check-examples.py [--smugglr PATH] [example-dir ...]
Defaults to the three credential-free CLI examples and `smugglr` on PATH.
Exit 1 on any mismatch, with a diff.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXAMPLES = os.path.join(ROOT, "docs", "examples")
DEFAULT = ["cli-sqlite-to-sqlite", "cli-stash-file-relay", "cli-migrate"]

SETUP = {
    "cli-sqlite-to-sqlite": [
        "cp config.example.toml config.toml",
        "../westwind/make.sh ./local.db",
        "../westwind/make.sh --empty ./backup.db",
    ],
    # The README uses /tmp/smugglr-relay; the checker gives each run its own
    # relay directory ($RELAY, under the scratch copy) so two runs cannot
    # delete each other's relay mid-flight.
    "cli-stash-file-relay": [
        'mkdir -p "$RELAY"',
        "../westwind/make.sh ./machine-a.db",
        "../westwind/make.sh --empty ./machine-b.db",
        'sed "s#file:///tmp/smugglr-relay#file://$RELAY#" config.example.toml > config-a.toml',
        "sed 's/machine-a.db/machine-b.db/' config-a.toml > config-b.toml",
    ],
    "cli-migrate": [
        "../westwind/make.sh ./westwind.db",
        "mkdir -p migrations",
    ],
}

FENCE = re.compile(r"```[a-z]*\n(\$ .*?)```", re.S)


def blocks(readme: str):
    """Yield (command, expected_stdout) for every `$ ...` fenced block."""
    for m in FENCE.finditer(readme):
        body = m.group(1)
        lines = body.split("\n")
        i = 0
        while i < len(lines):
            if not lines[i].startswith("$ "):
                i += 1
                continue
            cmd = lines[i][2:]
            i += 1
            out = []
            while i < len(lines) and not lines[i].startswith("$ "):
                out.append(lines[i])
                i += 1
            yield cmd, "\n".join(out).strip("\n")


def normalize(text: str):
    """Sorted non-empty lines, with JSON lines canonicalized."""
    norm = []
    for line in text.split("\n"):
        line = line.rstrip()
        if not line:
            continue
        if line.startswith("{"):
            try:
                obj = json.loads(line)
                if isinstance(obj.get("tables"), list):
                    obj["tables"] = sorted(obj["tables"], key=lambda t: t.get("name", ""))
                line = json.dumps(obj, sort_keys=True)
            except json.JSONDecodeError:
                pass
        norm.append(line)
    return sorted(norm)


def run(cmd: str, cwd: str, smugglr: str) -> tuple[str, int]:
    """Run one README command in the example directory; return (stdout, exit code).

    The commands come from this repository's own README files, which is why
    shell=True is acceptable here: redirects and pipes in those blocks are
    part of what a reader types.
    """
    env = dict(os.environ, SMUGGLR=smugglr, RELAY=os.path.join(cwd, "relay"))
    shell_cmd = re.sub(r"(?<![\w/])smugglr(?= )", smugglr, cmd)
    proc = subprocess.run(shell_cmd, shell=True, cwd=cwd, env=env, capture_output=True, text=True)
    return proc.stdout.strip("\n"), proc.returncode


def check(example: str, smugglr: str) -> bool:
    src = os.path.join(EXAMPLES, example)
    readme = open(os.path.join(src, "README.md")).read()
    with tempfile.TemporaryDirectory() as tmp:
        work = os.path.join(tmp, "examples")
        shutil.copytree(EXAMPLES, work, ignore=shutil.ignore_patterns("*.db", "node_modules", "target"))
        cwd = os.path.join(work, example)
        for step in SETUP.get(example, []):
            _, code = run(step, cwd, smugglr)
            if code != 0:
                print(f"SETUP FAILED in {example}: {step} (exit {code})")
                return False
        ok = True
        found = list(blocks(readme))
        if not found:
            # A README the regex cannot read would otherwise pass vacuously.
            print(f"NO BLOCKS in {example}: README has no `$ ...` fenced block the checker reads")
            return False
        for cmd, expected in found:
            # A mutation shown for its side effect prints nothing; run it so
            # the next block sees the changed rows, and compare nothing.
            if cmd.startswith("sqlite3") and "UPDATE" in cmd:
                run(cmd, cwd, smugglr)
                continue
            got, code = run(cmd, cwd, smugglr)
            # Every documented block succeeds; a README that shows a failing
            # command is not a shape these examples use.
            if code != 0:
                ok = False
                print(f"EXIT {code} in {example}: $ {cmd}")
            if normalize(got) != normalize(expected):
                ok = False
                print(f"MISMATCH in {example}: $ {cmd}")
                print("  expected:")
                for line in normalize(expected):
                    print("    " + line[:160])
                print("  got:")
                for line in normalize(got):
                    print("    " + line[:160])
        print(f"{'ok' if ok else 'FAIL'}: {example}")
        return ok


def readme_blocks_are_from_examples() -> bool:
    """Every fenced block in the root README must be a substring of some file
    under docs/examples/. The README copies; it never types. Blocks tagged
    `sh` are commands a reader runs and are exempt; everything else is a
    config or an output block and must have a source."""
    readme = open(os.path.join(ROOT, "README.md")).read()
    sources = []
    for dp, _, fs in os.walk(EXAMPLES):
        for f in fs:
            if f.endswith((".md", ".toml", ".sql", ".json")):
                sources.append(open(os.path.join(dp, f), errors="replace").read())
    ok = True
    for lang, body in re.findall(r"```([a-z]*)\n(.*?)```", readme, re.S):
        if lang == "sh":
            continue
        if not any(body in s for s in sources):
            ok = False
            print("README block has no source under docs/examples/:")
            print("    " + body.strip().split("\n")[0][:120])
    print(f"{'ok' if ok else 'FAIL'}: README blocks are from docs/examples")
    return ok


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--smugglr", default=shutil.which("smugglr") or "smugglr")
    ap.add_argument("--readme", action="store_true", help="only check that README blocks come from docs/examples")
    ap.add_argument("examples", nargs="*", default=DEFAULT)
    args = ap.parse_args()
    if args.readme:
        return 0 if readme_blocks_are_from_examples() else 1
    results = [check(e, args.smugglr) for e in args.examples]
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())

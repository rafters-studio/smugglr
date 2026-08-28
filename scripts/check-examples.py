#!/usr/bin/env python3
"""Run the credential-free CLI examples and compare their output to their READMEs.

Every fenced block in an example README that starts with `$ smugglr ...` or
`$ sqlite3 ...` is a claim: run this, see that. This script re-runs each
example's setup and every such block in a scratch copy of the example
directory and checks the claim.

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
    "cli-stash-file-relay": [
        "rm -rf /tmp/smugglr-relay && mkdir -p /tmp/smugglr-relay",
        "../westwind/make.sh ./machine-a.db",
        "../westwind/make.sh --empty ./machine-b.db",
        "cp config.example.toml config-a.toml",
        "sed 's/machine-a.db/machine-b.db/' config.example.toml > config-b.toml",
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


def run(cmd: str, cwd: str, smugglr: str) -> str:
    env = dict(os.environ, SMUGGLR=smugglr)
    shell_cmd = re.sub(r"(?<![\w/])smugglr(?= )", smugglr, cmd)
    proc = subprocess.run(shell_cmd, shell=True, cwd=cwd, env=env, capture_output=True, text=True)
    return proc.stdout.strip("\n")


def check(example: str, smugglr: str) -> bool:
    src = os.path.join(EXAMPLES, example)
    readme = open(os.path.join(src, "README.md")).read()
    with tempfile.TemporaryDirectory() as tmp:
        work = os.path.join(tmp, "examples")
        shutil.copytree(EXAMPLES, work, ignore=shutil.ignore_patterns("*.db", "node_modules", "target"))
        cwd = os.path.join(work, example)
        for step in SETUP.get(example, []):
            run(step, cwd, smugglr)
        ok = True
        for cmd, expected in blocks(readme):
            if cmd.startswith("sqlite3") and "UPDATE" in cmd:
                run(cmd, cwd, smugglr)
                continue
            got = run(cmd, cwd, smugglr)
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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--smugglr", default=shutil.which("smugglr") or "smugglr")
    ap.add_argument("examples", nargs="*", default=DEFAULT)
    args = ap.parse_args()
    results = [check(e, args.smugglr) for e in args.examples]
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())

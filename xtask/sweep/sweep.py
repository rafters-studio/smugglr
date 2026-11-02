#!/usr/bin/env python3
"""Toy X and Z's cases on real smugglr mesh processes.

Five nodes, unicast through a proxy that drops 10% of datagrams, a short sunset,
some nodes asleep past it, rows inserted, edited and deleted throughout. At the
end every key that differs must be named in the record of a node; anything else
is silent. Also counts false stops and rows that came back.

Usage: sweep.py <binary> <layout> <seed> <base-port> <out-dir>
Layouts: one, two, later, three (sleepers past the sunset), toomuch, inside.
Prints one JSON line."""
import json
import os
import random
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import threading
import time

binary, layout, seed, base, out = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5]
rng = random.Random(seed)
NODES = 5
SUNSET = int(os.environ.get("SUNSET", "12"))
LOSS = 0.10
root = os.path.join(out, f"{layout}-{seed}")
shutil.rmtree(root, ignore_errors=True)
os.makedirs(root)
port_of = [base + i for i in range(NODES)]
proxy_of = {}
next_port = base + NODES
for i in range(NODES):
    for j in range(NODES):
        if i != j:
            proxy_of[(i, j)] = next_port
            next_port += 1


def proxy():
    """Each pair has a socket: what i sends to it reaches j as if from j's view of i."""
    socks = {}
    for pair, port in proxy_of.items():
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
        s.bind(("127.0.0.1", port))
        s.settimeout(0.05)
        socks[pair] = s
    drop = random.Random(seed + 1)

    def pump(i, j):
        s = socks[(i, j)]
        back = socks[(j, i)]
        while not stopping.is_set():
            try:
                data, _ = s.recvfrom(65536)
            except OSError:
                continue
            if drop.random() >= LOSS:
                try:
                    back.sendto(data, ("127.0.0.1", port_of[j]))
                except OSError:
                    pass

    for pair in proxy_of:
        threading.Thread(target=pump, args=pair, daemon=True).start()


stopping = threading.Event()
proxy()


class Node:
    def __init__(self, i):
        self.i, self.name = i, f"n{i}"
        self.dir = os.path.join(root, self.name)
        os.makedirs(os.path.join(self.dir, "bin"))
        os.makedirs(os.path.join(self.dir, "config", "smugglr"))
        host = os.path.join(self.dir, "bin", "hostname")
        open(host, "w").write(f"#!/bin/sh\necho {self.name}\n")
        os.chmod(host, 0o755)
        peers = [f"127.0.0.1:{proxy_of[(i, j)]}" for j in range(NODES) if j != i]
        extra = "mesh_wire = 1500\n" if layout == "toomuch" else ""
        open(os.path.join(self.dir, "config", "smugglr", "config.toml"), "w").write(
            f"port = {port_of[i]}\npeers = {json.dumps(peers)}\ngraveyard = {SUNSET}\n"
            f'steering = "updated_at"\n{extra}'
        )
        self.db = os.path.join(self.dir, "n.db")
        c = sqlite3.connect(self.db)
        c.execute("CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT, updated_at TEXT)")
        c.executemany("INSERT INTO notes VALUES (?,?,?)",
                      [(f"r{k:03}", "base", "2026-01-01T00:00:00.000") for k in range(30)])
        c.commit()
        c.close()
        self.log = os.path.join(self.dir, "log")
        self.p = None

    def start(self):
        env = dict(os.environ, PATH=os.path.join(self.dir, "bin") + ":" + os.environ["PATH"],
                   XDG_CONFIG_HOME=os.path.join(self.dir, "config"))
        self.p = subprocess.Popen(
            [binary, "mesh", self.db, "--key-file", key, "--sync-mode", "last"],
            env=env, stdout=open(self.log, "a"), stderr=subprocess.STDOUT)

    def stop(self):
        if self.p:
            self.p.terminate()
            self.p.wait()
            self.p = None

    def rows(self):
        c = sqlite3.connect(self.db, timeout=10)
        out = {r[0]: (r[1], r[2]) for r in c.execute("SELECT id, body, updated_at FROM notes")}
        c.close()
        return out

    def write(self, sql, args):
        c = sqlite3.connect(self.db, timeout=10)
        c.execute(sql, args)
        c.commit()
        c.close()

    def printed(self):
        return open(self.log, errors="replace").read() if os.path.exists(self.log) else ""


key = os.path.join(root, "mesh.key")
subprocess.run([binary, "mesh", "keygen", "--key-file", key], check=True, stdout=subprocess.DEVNULL)
os.chmod(key, 0o600)
nodes = [Node(i) for i in range(NODES)]
for n in nodes:
    n.start()
time.sleep(4)

clock = {"n": 0}
deleted, inserted = set(), 0


def stamp():
    clock["n"] += 1
    return f"2026-02-01T00:00:00.{clock['n']:06}"


def work(awake, count):
    global inserted
    for _ in range(count):
        n = rng.choice(awake)
        roll = rng.random()
        rows = n.rows()
        try:
            if roll < 0.4:
                inserted += 1
                n.write("INSERT INTO notes VALUES (?,?,?)", (f"i{n.i}-{inserted}", "new", stamp()))
            elif roll < 0.85 and rows:
                k = rng.choice(sorted(rows))
                n.write("UPDATE notes SET body=?, updated_at=? WHERE id=?", (f"e{n.i}", stamp(), k))
            elif rows:
                k = rng.choice(sorted(rows))
                n.write("DELETE FROM notes WHERE id=?", (k,))
                deleted.add(k)
        except sqlite3.Error:
            pass
        time.sleep(0.05)


sleepers = {"one": [4], "two": [3, 4], "later": [3, 4], "three": [2, 3, 4], "toomuch": [4], "inside": [4]}[layout]
work(nodes, 20)
time.sleep(2)
asleep = SUNSET // 2 if layout in ("inside", "toomuch") else SUNSET + 6
if layout == "later":
    nodes[3].stop()
    work([n for n in nodes if n.i != 3], 15)
    time.sleep(3)
    nodes[4].stop()
else:
    for i in sleepers:
        nodes[i].stop()
awake = [n for n in nodes if n.p]
started = time.time()
if layout == "toomuch":
    # More than the wire's line, so the peers answer too much.
    for k in range(60):
        awake[0].write("INSERT INTO notes VALUES (?,?,?)", (f"m{k}", "x" * 40, stamp()))
while time.time() - started < asleep:
    work(awake, 5)
    time.sleep(0.5)
for i in sleepers:
    nodes[i].start()
    if layout == "later":
        time.sleep(3)
    work(awake, 3)

# Quiet, then wait for the mesh to settle or give up.
deadline = time.time() + max(60, SUNSET * 2)
same = False
while time.time() < deadline:
    time.sleep(3)
    views = [n.rows() for n in nodes]
    same = all(v == views[0] for v in views)
    if same:
        break
elapsed = max(60, SUNSET * 2) - max(0, deadline - time.time())
views = [n.rows() for n in nodes]
keys = set().union(*views)
wrong = {k for k in keys if len({json.dumps(v.get(k)) for v in views}) > 1}
printed = {n.name: n.printed() for n in nodes}
for n in nodes:
    n.stop()
stopping.set()
everything = "\n".join(printed.values())
named = set()
lines = [l for l in everything.splitlines() if "has not acknowledged" in l and "settle nothing" in l]
for line in lines:
    named.update(re.findall(r"\[\"'([^']+)'\"\]", line))
truncated = sum("more" in l.rsplit(" and ", 1)[-1] for l in lines)
silent = wrong - named
stops = re.findall(r"stopped notes \[\"'([^']+)'\"\]", everything)
named.update(stops)
named.update(re.findall(r"dropped a change to notes \[\"'([^']+)'\"\]", everything))
silent = wrong - named
false_stops = everything.count("heard a change under the name")
back = {k for k in deleted if any(k in v for v in views)} - named
print(json.dumps({
    "layout": layout, "seed": seed, "whole": same, "settle_s": round(elapsed), "keys": len(keys),
    "wrong": len(wrong), "silent": len(silent), "named": len(named), "named_lines": len(lines), "conflict_stops": len(set(stops)),
    "truncated_lines": truncated, "false_stops": false_stops, "back": len(back),
    "stash_taken": everything.count("caught up by a stash"), "too_much": everything.count("too much for the wire"),
    "panic": everything.count("panicked"),
    "silent_keys": sorted(silent)[:40], "rows_by_node": {k: [json.dumps(v.get(k)) for v in views] for k in sorted(silent)[:3]}, "back_keys": sorted(back)[:5],
}))
if silent and os.environ.get("KEEP"):
    import glob
    for k in sorted(silent)[:2]:
        print("@@ TRAIL", k, "deleted_by_workload", k in deleted, "now", int(time.time()), file=sys.stderr)
        q = "'" + k + "'"
        for n in nodes:
            base = os.path.join(n.dir, "config", "smugglr", "mesh")
            for f in glob.glob(base + "/*/rows.jsonl"):
                for l in open(f, errors="replace"):
                    if q in l:
                        v = json.loads(l)
                        if "b" in v or "f" in v:
                            print("  ", n.name, "journal", l.strip()[:110], file=sys.stderr)
                        elif "t" in v:
                            print("  ", n.name, "tombstone from", v["t"]["from"], "by", v["t"]["node"], "sunset", v["t"]["sunset"], file=sys.stderr)
                        elif "d" in v:
                            print("  ", n.name, "dropped log", v["d"]["node"], v["d"]["from"], v["d"]["to"], "sunset", v["d"]["sunset"], file=sys.stderr)
            acks = set()
            for f in glob.glob(base + "/*/outbox/acks.jsonl"):
                for l in open(f):
                    v = json.loads(l)
                    acks.add((v["c"], v["n"]))
            for f in glob.glob(base + "/*/outbox/0*.jsonl"):
                for l in open(f):
                    v = json.loads(l)
                    e = v.get("e")
                    if e and q in e["key"]:
                        print("  ", n.name, "outbox", str(e["from"])[:8], "->", str(e["to"])[:8], "sunset", e["sunset"], "targets", v.get("t"), "acked", sorted(p for c, p in acks if c == e["signature"]), file=sys.stderr)
if silent and os.environ.get("KEEP"):
    for name, text in printed.items():
        print("==", name, file=sys.stderr)
        print("\n".join(l[:190] for l in text.splitlines() if "arrived only" not in l and "resent " not in l)[-int(os.environ["KEEP"]):], file=sys.stderr)
shutil.rmtree(root, ignore_errors=True)

#!/usr/bin/env python3
"""Runs sweep.py over layouts and seeds, four at a time, and sums the results.

Usage: drive.py <binary> <results.jsonl> <seeds> <layouts>
Layouts: one,two,later,three,toomuch,inside. SUNSET in the environment sets the
sunset in seconds, 12 when not set. A run with a silent row writes its records and
each silent key's history to <results.jsonl>.records."""
import json, os, subprocess, sys, tempfile, threading, queue
HERE = os.path.dirname(os.path.abspath(__file__))
WORK = tempfile.mkdtemp(prefix="smugglr-sweep-")
binary, out_file, seeds = os.path.abspath(sys.argv[1]), os.path.abspath(sys.argv[2]), int(sys.argv[3])
layouts = sys.argv[4].split(",")
q = queue.Queue()
for s in range(1, seeds + 1):
    for l in layouts:
        q.put((l, s))
ports = iter(range(43000, 60000, 60))
lock = threading.Lock()
def worker():
    while True:
        try:
            l, s = q.get_nowait()
        except queue.Empty:
            return
        with lock:
            base = next(ports)
        r = subprocess.run([sys.executable, "-I", os.path.join(HERE, "sweep.py"), binary, l, str(s), str(base), WORK],
                           capture_output=True, text=True, env=dict(os.environ, KEEP="200000"))
        line = r.stdout.strip().splitlines()[-1] if r.stdout.strip() else json.dumps({"layout": l, "seed": s, "crash": r.stderr[-300:]})
        with lock:
            open(out_file, "a").write(line + "\n")
            if r.stderr.strip():
                open(out_file + ".records", "a").write(f"#### {l} {s}\n{r.stderr}\n")
ts = [threading.Thread(target=worker) for _ in range(4)]
[t.start() for t in ts]; [t.join() for t in ts]
rows = [json.loads(x) for x in open(out_file)]
for l in layouts:
    rs = [r for r in rows if r.get("layout") == l and "crash" not in r]
    if rs:
        f = lambda k: sum(r[k] for r in rs)
        print(l, "runs", len(rs), "whole", sum(r["whole"] for r in rs), "wrong", f("wrong"), "silent", f("silent"), "named", f("named"),
              "conflict_stops", f("conflict_stops"), "false_stops", f("false_stops"), "back", f("back"), "stash", f("stash_taken"), "panic", f("panic"))
print("crashes", sum("crash" in r for r in rows))

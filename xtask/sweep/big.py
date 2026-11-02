import os, shutil, sqlite3, subprocess, sys, time, resource
binary, rows, root = sys.argv[1], int(sys.argv[2]), sys.argv[3]
shutil.rmtree(root, ignore_errors=True); os.makedirs(root)
ports = (47100, 47101)
def node(name, port, peer):
    d = os.path.join(root, name); os.makedirs(d + "/bin"); os.makedirs(d + "/config/smugglr")
    open(d + "/bin/hostname", "w").write(f"#!/bin/sh\necho {name}\n"); os.chmod(d + "/bin/hostname", 0o755)
    open(d + "/config/smugglr/config.toml", "w").write(f'port = {port}\npeers = ["127.0.0.1:{peer}"]\ngraveyard = 90\nsteering = "updated_at"\n')
    c = sqlite3.connect(d + "/n.db"); c.execute("CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT, updated_at TEXT)"); c.commit(); c.close()
    return d
a, b = node("a", ports[0], ports[1]), node("b", ports[1], ports[0])
key = root + "/k"; subprocess.run([binary, "mesh", "keygen", "--key-file", key], check=True, stdout=subprocess.DEVNULL); os.chmod(key, 0o600)
def start(d):
    env = dict(os.environ, PATH=d + "/bin:" + os.environ["PATH"], XDG_CONFIG_HOME=d + "/config")
    return subprocess.Popen([binary, "mesh", d + "/n.db", "--key-file", key, "--sync-mode", "last"], env=env, stdout=open(d + "/log", "a"), stderr=subprocess.STDOUT)
pa, pb = start(a), start(b); time.sleep(3)
pa.terminate(); pa.wait()
c = sqlite3.connect(b + "/n.db")
c.executemany("INSERT INTO notes VALUES (?,?,?)", ((f"r{k:07}", "x" * 500, "2026-05-01") for k in range(rows))); c.commit(); c.close()
size = os.path.getsize(b + "/n.db")
pb.terminate(); pb.wait(); pb = start(b)
# Long enough to pass the 8 s sunset, with b's first look noting nothing new to send.
time.sleep(95)
t0 = time.time(); pa = start(a)
maxrss = {"a": 0, "b": 0}
def rss(pid):
    try: return int(subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True).stdout.strip() or 0) // 1024
    except ValueError: return 0
done = False
while time.time() - t0 < 500:
    time.sleep(2)
    maxrss["a"] = max(maxrss["a"], rss(pa.pid)); maxrss["b"] = max(maxrss["b"], rss(pb.pid))
    if "caught up by a stash" in open(a + "/log").read(): done = True; break
el = time.time() - t0
got = sqlite3.connect(a + "/n.db").execute("SELECT count(*) FROM notes").fetchone()[0] if done else -1
print({"rows": rows, "store_mb": size // 1_000_000, "caught_up": done, "seconds": round(el), "rows_held": got, "peak_rss_mb": maxrss})
print("\n".join(l[:150] for l in open(a + "/log").read().splitlines()[-4:])); print("\n".join(l[:150] for l in open(b + "/log").read().splitlines()[-4:]))
pa.terminate(); pb.terminate(); shutil.rmtree(root, ignore_errors=True)

"""A minimal HTTP-SQL endpoint over one SQLite file, speaking smugglr's
`generic` profile: POST a JSON body `{"sql": ..., "params": [...]}` and get
back `{"columns": [...], "rows": [[...], ...]}`.

Standard library only. It stands in for rqlite, Turso, or your own gateway
so the service in this directory has something to sync against.

    python3 serve.py remote.db 18787
"""

import json
import sqlite3
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class SqlHandler(BaseHTTPRequestHandler):
    db_path = "remote.db"

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
            sql = body["sql"]
            params = body.get("params", [])
        except (ValueError, KeyError) as err:
            return self.reply(400, {"error": f"bad request: {err}"})

        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.execute(sql, params)
            columns = [d[0] for d in cur.description] if cur.description else []
            rows = [list(r) for r in cur.fetchall()]
            conn.commit()
        except sqlite3.Error as err:
            return self.reply(400, {"error": str(err)})
        finally:
            conn.close()
        self.reply(200, {"columns": columns, "rows": rows})

    def reply(self, status, payload):
        data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):
        sys.stderr.write("serve.py: " + fmt % args + "\n")


if __name__ == "__main__":
    SqlHandler.db_path = sys.argv[1] if len(sys.argv) > 1 else "remote.db"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 18787
    sys.stderr.write(f"serve.py: {SqlHandler.db_path} on http://127.0.0.1:{port}/sql\n")
    ThreadingHTTPServer(("127.0.0.1", port), SqlHandler).serve_forever()

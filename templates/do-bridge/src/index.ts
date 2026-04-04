// smugglr DO Bridge -- exposes a Durable Object's SQLite storage as a D1-compatible HTTP SQL endpoint.
//
// Deploy this Worker to make any DO syncable with smugglr using the existing D1 adapter.
// See the README for configuration.

export interface Env {
  DO_BRIDGE: DurableObjectNamespace;
  AUTH_TOKEN: string;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (request.method !== "POST") {
      return new Response(
        JSON.stringify({ success: false, errors: [{ message: "Method not allowed" }] }),
        { status: 405, headers: { "Content-Type": "application/json" } },
      );
    }

    const token = request.headers.get("Authorization")?.replace("Bearer ", "");
    if (!env.AUTH_TOKEN || token !== env.AUTH_TOKEN) {
      return new Response(
        JSON.stringify({ success: false, errors: [{ message: "Unauthorized" }] }),
        { status: 401, headers: { "Content-Type": "application/json" } },
      );
    }

    // Route to a single DO instance. The ID is deterministic so all requests
    // hit the same object (one bridge per DO, per the issue spec).
    const id = env.DO_BRIDGE.idFromName("default");
    const stub = env.DO_BRIDGE.get(id);
    return stub.fetch(request);
  },
};

export class SqliteBridge implements DurableObject {
  private ctx: DurableObjectState;

  constructor(ctx: DurableObjectState, _env: Env) {
    this.ctx = ctx;
  }

  async fetch(request: Request): Promise<Response> {
    try {
      const body = (await request.json()) as { sql?: string; params?: unknown[] };

      if (!body.sql) {
        return jsonResponse(
          { success: false, errors: [{ message: "Missing 'sql' field in request body" }] },
          400,
        );
      }

      const sql = body.sql;
      const params = body.params ?? [];
      const cursor = this.ctx.storage.sql.exec(sql, ...params);

      // Read all rows as {column: value} objects to match D1 response format.
      const columns = cursor.columnNames;
      const rows: Record<string, unknown>[] = [];
      for (const row of cursor.raw()) {
        const obj: Record<string, unknown> = {};
        for (let i = 0; i < columns.length; i++) {
          obj[columns[i]] = row[i];
        }
        rows.push(obj);
      }

      return jsonResponse({
        result: [
          {
            results: rows,
            success: true,
            meta: {
              changed_db: cursor.rowsWritten > 0,
              changes: cursor.rowsWritten,
              rows_read: cursor.rowsRead,
              rows_written: cursor.rowsWritten,
            },
          },
        ],
        success: true,
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return jsonResponse(
        { success: false, errors: [{ message, code: 1 }] },
        400,
      );
    }
  }
}

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

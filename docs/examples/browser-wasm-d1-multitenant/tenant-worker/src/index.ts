// Tenant guard in front of D1.
//
// Speaks D1's REST shape so a smugglr client using `profile: "d1"` can point
// straight at this Worker's URL. The Worker authenticates the Bearer token,
// resolves it to a tenant_id, rewrites SELECTs to scope by tenant, and
// validates that every INSERT carries the authenticated tenant_id.

export interface Env {
  DB: D1Database;
  TENANT_TOKEN_ALICE: string;
  TENANT_TOKEN_BOB: string;
}

interface Body {
  sql: string;
  params?: unknown[];
}

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Authorization, Content-Type",
};

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    if (req.method === "OPTIONS") return new Response(null, { headers: CORS });
    if (req.method !== "POST") return json({ error: "POST only" }, 405);

    const tenant = resolveTenant(req, env);
    if (!tenant) return json({ error: "unauthorized" }, 401);

    let body: Body;
    try {
      body = await req.json();
    } catch {
      return json({ error: "invalid json" }, 400);
    }

    try {
      const rewritten = enforce(body.sql, body.params ?? [], tenant);
      const stmt = env.DB.prepare(rewritten.sql).bind(...rewritten.params);
      const result = await stmt.all();
      // Match the D1 REST response shape that smugglr's d1 profile expects:
      //   { result: [ { results: [...], success: true, meta: {...} } ] }
      return json({
        result: [
          {
            results: result.results ?? [],
            success: true,
            meta: result.meta ?? {},
          },
        ],
        success: true,
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return json({ error: message }, 400);
    }
  },
};

function resolveTenant(req: Request, env: Env): string | null {
  const header = req.headers.get("authorization") ?? "";
  const token = header.replace(/^Bearer\s+/i, "").trim();
  if (!token) return null;
  if (token === env.TENANT_TOKEN_ALICE) return "alice";
  if (token === env.TENANT_TOKEN_BOB) return "bob";
  return null;
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", ...CORS },
  });
}

// Rewrite the incoming SQL so the authenticated tenant cannot see or write
// another tenant's rows. Three cases cover everything smugglr's d1 adapter
// (crates/smugglr-wasm/src/fetch_adapter.rs) emits against the dest:
//
//   SELECT name FROM sqlite_master WHERE type='table' ...     table discovery
//   PRAGMA table_info('notes')                                column + pk discovery
//   SELECT *, CAST("id" AS TEXT) AS __pk FROM "notes" ...     row metadata
//   SELECT * FROM "notes" WHERE CAST("id" AS TEXT) IN (?, ...) row fetch
//   INSERT OR REPLACE INTO "notes" ("id", ...) VALUES (...)   push
function enforce(
  rawSql: string,
  params: unknown[],
  tenant: string,
): { sql: string; params: unknown[] } {
  const sql = rawSql.trim().replace(/;\s*$/, "");
  const head = sql.slice(0, 32).toUpperCase();

  // Passthrough: schema discovery reads no tenant rows.
  if (/FROM\s+SQLITE_MASTER/i.test(sql)) return { sql, params };
  if (/^PRAGMA\s+TABLE_INFO\s*\(/i.test(sql)) return { sql, params };

  if (head.startsWith("SELECT")) {
    // Wrap the query so we can filter without parsing it. Every row-reading
    // SELECT the adapter emits projects the table's columns, so the subquery
    // keeps tenant_id available to the outer WHERE. None of them names
    // tenant_id in its text, so a query that does is a client trying to
    // alias a literal over the real column and defeat the outer filter.
    if (/tenant_id/i.test(sql)) {
      throw new Error("SELECT must not name tenant_id; the guard adds the filter");
    }
    return {
      sql: `SELECT * FROM (${sql}) WHERE tenant_id = ?`,
      params: [...params, tenant],
    };
  }

  if (head.startsWith("INSERT")) {
    // smugglr push: INSERT OR REPLACE INTO "table" ("col1", "col2", ...) VALUES (?, ?, ...), (?, ?, ...)
    const m = sql.match(/INSERT\s+(?:OR\s+\w+\s+)?INTO\s+"?([^"\s(]+)"?\s*\(([^)]+)\)/i);
    if (!m) throw new Error("cannot parse INSERT columns");
    const cols = m[2].split(",").map((c) => c.trim().replace(/^"|"$/g, ""));
    const tenantIdx = cols.indexOf("tenant_id");
    if (tenantIdx < 0) {
      throw new Error("INSERT must include tenant_id column for this guard");
    }
    // The adapter binds every value, so a params-less INSERT (INSERT ...
    // SELECT) never reaches the per-row check below; refuse the shape.
    if (params.length === 0 || /\bSELECT\b/i.test(sql)) {
      throw new Error("INSERT must bind its rows as parameters; INSERT ... SELECT is not accepted");
    }
    if (params.length % cols.length !== 0) {
      throw new Error("param count not a multiple of column count");
    }
    for (let i = tenantIdx; i < params.length; i += cols.length) {
      if (params[i] !== tenant) {
        throw new Error(
          `tenant_id mismatch in row ${i / cols.length}: got ${String(params[i])}, expected ${tenant}`,
        );
      }
    }
    return { sql, params };
  }

  throw new Error(`statement not allowed: ${head.split(/\s/)[0]}`);
}

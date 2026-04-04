# smugglr DO Bridge

A Cloudflare Worker template that exposes a Durable Object's SQLite storage as a D1-compatible HTTP SQL endpoint. This turns any DO into a smugglr sync target without a custom adapter.

## How it works

The Worker receives SQL queries via HTTP POST in the same format as the Cloudflare D1 API. It routes them to a Durable Object, runs them via the DO SQLite storage API, and returns results in D1 response format. Because the API shape matches D1, smugglr's existing D1 adapter works against the bridge with zero changes.

## Deploy

```bash
cd templates/do-bridge
npm install
# Set your auth token as a secret
wrangler secret put AUTH_TOKEN
# Deploy
wrangler deploy
```

## Configure smugglr

Point smugglr at your deployed bridge using the D1 target type:

```toml
[target]
type = "d1"
account_id = "unused"
database_id = "unused"
api_token = "your-auth-token"
```

Then override the endpoint URL in your smugglr config or set the `SMUGGLR_D1_ENDPOINT` environment variable to your Worker URL:

```
https://do-bridge.<your-subdomain>.workers.dev
```

Note: `account_id` and `database_id` are required by the D1 config schema but are not used by the bridge -- set them to any non-empty string.

## Request format

```
POST / HTTP/1.1
Authorization: Bearer <your-token>
Content-Type: application/json

{"sql": "SELECT * FROM users WHERE id = ?", "params": [1]}
```

## Response format

Matches the Cloudflare D1 API:

```json
{
  "result": [{
    "results": [{"id": 1, "name": "alice"}],
    "success": true,
    "meta": {
      "changed_db": false,
      "changes": 0,
      "rows_read": 1,
      "rows_written": 0
    }
  }],
  "success": true
}
```

## Limitations

- One bridge per Durable Object (no multi-DO routing)
- SQL only (no DO alarms, KV, or WebSocket features)
- No rate limiting (the DO handles its own concurrency)

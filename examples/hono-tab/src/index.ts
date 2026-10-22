// A Hono Worker a tab keeps in step with (FR-SMUGGLR-062): it answers the
// place operations @smugglr/web's fetcher asks, over the D1 binding, for the
// signed-in person's rows only, and makes the stash blob a tab fills itself
// from with retrieve() (FR-SMUGGLR-077). smugglr ships no server; this is
// app code, one way to write it.
//
// What it reaches: D1 through its binding, and nothing else. It holds no
// Cloudflare API token. The session check and the signing key are the app's.

import { Hono } from "hono";
import { answer, Refused, tables } from "./place";
import { stash } from "./stash";

type Bindings = {
  // The app's database: the people's rows, and its sessions.
  DB: D1Database;
  // The app's own key for stash blobs, 64 hex digits, a Worker secret
  // (FR-SMUGGLR-078: the app chooses the key). Its tabs retrieve with it.
  SMUGGLR_KEY: string;
};

const app = new Hono<{ Bindings: Bindings; Variables: { owner: string } }>();

// The app's session check goes here. This one is a stub: a bearer token
// looked up in a `sessions` table of the same D1, naming the person.
app.use("*", async (c, next) => {
  const token = c.req.header("authorization")?.match(/^Bearer (.+)$/)?.[1];
  const owner =
    token === undefined
      ? null
      : await c.env.DB.prepare("SELECT person FROM sessions WHERE token = ?1").bind(token).first<string>("person");
  if (owner === null) {
    return c.text("unauthorized", 401);
  }
  c.set("owner", owner);
  await next();
});

// A refusal says why; any other failure is logged here and the client
// hears only that it failed, never D1's own words.
app.onError((err, c) => {
  if (err instanceof Refused) {
    return c.text(err.message, err.status);
  }
  console.error(err);
  return c.text("internal error", 500);
});

// The endpoint a tab names in open(): one place operation a request.
app.post("/place", async (c) => {
  const request = await c.req.json<unknown>().catch(() => {
    throw new Refused(400, "the body is not JSON");
  });
  const answered = await answer(c.env.DB, c.get("owner"), request);
  return c.body(JSON.stringify(answered), 200, { "content-type": "application/json" });
});

// A stash blob of the person's rows, for the device named, which fills its
// tab with retrieve() and then syncs with /place.
app.get("/stash/:device", async (c) => {
  const blob = await stash(c.env.DB, await tables(c.env.DB), c.get("owner"), {
    key: c.env.SMUGGLR_KEY,
    node: "server",
    target: c.req.param("device"),
  });
  return c.body(blob, 200, { "content-type": "application/json" });
});

export default app;

// A Hono Worker that keeps a person's blobs for smugglr multi: the bucket,
// the device list, crossing off, and deleting a blob with no targets left.
// smugglr makes and applies the blobs; everything here is app code.

import { Hono } from "hono";

type Bindings = {
  // The bucket: one object per blob, plus the device list.
  BUCKET: R2Bucket;
  // The one token the person's devices send.
  TOKEN: string;
};

const DEVICES = "devices.json";

const app = new Hono<{ Bindings: Bindings }>();

app.use("*", async (c, next) => {
  if (c.req.header("authorization") !== `Bearer ${c.env.TOKEN}`) {
    return c.text("unauthorized", 401);
  }
  await next();
});

function strings(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((v): v is string => typeof v === "string") : [];
}

async function devices(bucket: R2Bucket): Promise<string[]> {
  const object = await bucket.get(DEVICES);
  return object ? strings(await object.json<unknown>()) : [];
}

// The device list. A new device adds itself, then catches up from a whole
// stash another device makes for it (smugglr stash --whole --target <it>).
app.get("/devices", async (c) => c.json(await devices(c.env.BUCKET)));

app.put("/devices/:device", async (c) => {
  const list = await devices(c.env.BUCKET);
  const device = c.req.param("device");
  if (!list.includes(device)) list.push(device);
  await c.env.BUCKET.put(DEVICES, JSON.stringify(list));
  return c.json(list);
});

// A device puts the blob `smugglr stash` wrote. Its targets are the devices
// it named with --target; the bucket keeps who has not crossed it off yet.
app.put("/blobs/:name", async (c) => {
  const targets = strings(c.req.query("targets")?.split(","));
  await c.env.BUCKET.put(`blobs/${c.req.param("name")}`, await c.req.arrayBuffer(), {
    customMetadata: { targets: JSON.stringify(targets), crossed: "[]" },
  });
  return c.body(null, 201);
});

// The blobs that still list this device, each with the devices already
// crossed off, for `smugglr retrieve --crossed-off`.
app.get("/blobs/for/:device", async (c) => {
  const device = c.req.param("device");
  const listed = await c.env.BUCKET.list({ prefix: "blobs/", include: ["customMetadata"] });
  const blobs = listed.objects.flatMap((object) => {
    const meta = object.customMetadata ?? {};
    const targets = strings(JSON.parse(meta.targets ?? "[]"));
    const crossed = strings(JSON.parse(meta.crossed ?? "[]"));
    return targets.includes(device) && !crossed.includes(device)
      ? [{ name: object.key.slice("blobs/".length), crossed }]
      : [];
  });
  return c.json(blobs);
});

app.get("/blobs/:name", async (c) => {
  const object = await c.env.BUCKET.get(`blobs/${c.req.param("name")}`);
  return object ? c.body(object.body) : c.text("not found", 404);
});

// Crossing off: once `smugglr retrieve` says done, the device crosses itself
// off. A blob with no targets left is deleted.
app.post("/blobs/:name/cross-off/:device", async (c) => {
  const key = `blobs/${c.req.param("name")}`;
  const object = await c.env.BUCKET.get(key);
  if (!object) return c.text("not found", 404);
  const meta = object.customMetadata ?? {};
  const targets = strings(JSON.parse(meta.targets ?? "[]"));
  const crossed = strings(JSON.parse(meta.crossed ?? "[]"));
  const device = c.req.param("device");
  if (!crossed.includes(device)) crossed.push(device);
  const left = targets.filter((target) => !crossed.includes(target));
  if (left.length === 0) {
    await c.env.BUCKET.delete(key);
    return c.json({ deleted: true, left });
  }
  await c.env.BUCKET.put(key, object.body, {
    customMetadata: { targets: JSON.stringify(targets), crossed: JSON.stringify(crossed) },
  });
  return c.json({ deleted: false, left });
});

export default app;

# hono-multi

A person's own devices kept in step by `smugglr stash` and `smugglr retrieve`,
through a bucket the app owns. The hub never holds this data. smugglr makes
and applies blobs; the bucket, the device list, crossing off, keys and new
devices are app code, and this Worker (`src/index.ts`, Hono on Cloudflare
Workers with an R2 bucket) is one way to write them.

## The bucket

One R2 object per blob, under `blobs/`, and `devices.json`, the device list.
Each blob object keeps, beside the blob, its targets and the devices that have
crossed it off.

## The device list

```console
$ curl -X PUT -H "authorization: Bearer $TOKEN" $APP/devices/laptop
["phone","laptop"]
```

Each device names every other device as a target when it stashes.

## Stash

```console
$ smugglr stash ./notes.db ./outbox --key-file ./multi.key --node laptop --target phone
./outbox/<millis>-<hash>.blob
$ curl -X PUT -H "authorization: Bearer $TOKEN" --data-binary @./outbox/$BLOB \
    "$APP/blobs/$BLOB?targets=phone"
```

## Retrieve, then crossing off

```console
$ curl -H "authorization: Bearer $TOKEN" $APP/blobs/for/phone
[{"name":"$BLOB","crossed":[]}]
$ curl -H "authorization: Bearer $TOKEN" -o ./inbox/$BLOB \
    $APP/blobs/$BLOB
$ smugglr retrieve ./notes.db ./inbox --key-file ./multi.key --node phone --json
```

When the record's status is `done`, the device crosses itself off:

```console
$ curl -X POST -H "authorization: Bearer $TOKEN" \
    $APP/blobs/$BLOB/cross-off/phone
{"deleted":true,"left":[]}
```

Crossing off is the app's marker, not smugglr's: pass the devices already
crossed off to `smugglr retrieve --crossed-off <name>` and the record names
the targets that remain. A blob with no targets left is deleted. A blob that
is not done stays listed for the device, which retrieves it again with what
arrives next; blobs apply in any order.

## A new device, or one away past the sunset

A new device adds itself to the list. Any device that stayed in step makes a
whole stash for it, and it retrieves that:

```console
$ smugglr stash ./notes.db ./outbox --key-file ./multi.key --node laptop --target tablet --whole
```

The same goes for a device away longer than the sunset (the `graveyard`
setting): it keeps its own changes since its last stash and takes the whole
stash's rows and graveyard for everything else.

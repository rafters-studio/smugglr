# Encryption + TCP framing for cross-process sync envelope

**Issue:** #90
**Status:** draft, awaiting co-driver (legion) review
**Author:** smugglr

## Goal

Define a wire-compatible frame format for the TCP sync path in `smugglr-core::broadcast` so that smugglr and legion (embedding smugglr-core as a library dependency) can converge on a single implementation instead of each inventing their own framing.

## Non-goals

- Key negotiation or auth handshakes. The pre-shared key model is the scope.
- TLS. We are building on top of the existing XChaCha20-Poly1305 envelope.
- Changing UDP announcement packets. Peer discovery stays as-is.
- Multiplexing multiple logical streams on one TCP connection. One connection, one sync exchange.

## Current state

Two pieces exist in `crates/smugglr-core/src/broadcast.rs`, both live in the module but neither references the other:

**Encryption envelope (lines 179-257, all `#[allow(dead_code)]`):**
- `encrypt_packet(plaintext, key) -> Vec<u8>` produces `[24-byte nonce][ciphertext + 16-byte tag]`
- `decrypt_packet(data, key) -> Vec<u8>` reverses it
- `maybe_encrypt` / `maybe_decrypt` are the conditional wrappers for when `BroadcastConfig.secret` is `None`
- `XChaCha20-Poly1305` AEAD, 256-bit key derived from the hex-encoded `secret` field
- `ENCRYPTION_OVERHEAD = 24 + 16 = 40` bytes per packet

**TCP framing (lines 822-875, currently plaintext JSON only):**
- `write_framed<T: Serialize>` writes `[u32 BE length][JSON payload]`
- `read_framed<T: Deserialize>` reverses it
- Hard cap at 64 MB per frame
- Used by `handle_sync_connection` for `SyncRequest` / `SyncResponse` bodies

**Replay protection (lines 651-744):**
- `ReplayGuard` is a per-peer 64-sequence sliding bitfield window
- Already integrated in both the UDP delta path and the TCP sync path via the `replay_guard: &Arc<Mutex<ReplayGuard>>` parameter
- Rejects duplicate or too-old sequence numbers at the delta layer, not the frame layer

The missing piece is the composition: the TCP path currently sends plaintext JSON frames, and the encryption envelope has no caller. This spec defines how they meet.

## Design decisions

### 1. Per-frame encryption, not session streaming

Encrypt each frame independently using `encrypt_packet`. No stream cipher state carried across frames, no session keys.

**Why:** Composition is trivial -- the existing `encrypt_packet` already produces a self-contained blob with a random nonce, and we just wrap it in the existing length prefix. No new primitives. Reconnects are free: dropped and re-opened TCP connections don't lose any cipher state because there is none. The 24-byte nonce overhead per frame is negligible against multi-KB sync payloads (rows, delta packets).

**Trade-off:** We pay the AEAD setup cost per frame. Measured overhead for XChaCha20-Poly1305 on modern x86-64 is ~1 GB/s per core -- not a concern for LAN sync where the bottleneck is the network, not the cipher.

### 2. Per-frame random nonce, not counter

192-bit `XChaCha20` nonces are large enough that random generation is collision-safe even at high frame rates. The birthday bound is 2^96, so a single key can safely produce ~10^28 frames before nonce collision becomes statistically likely.

**Why not counter:** Counter nonces would save 0 bytes (the counter still needs to fit in the nonce field), require synchronized state on both sides, and complicate reconnect semantics. The random approach is stateless and matches what `encrypt_packet` already does.

### 3. Key material from `BroadcastConfig.secret`, no per-session derivation

Both sides read the same 256-bit key from config and use it directly. No HKDF, no per-session derivation, no rotation protocol.

**Why:** Scope. The pre-shared key model is a deliberate choice -- it matches the LAN sync use case where operators control both peers. Key rotation and forward secrecy are out of scope for this spec and would warrant a separate proposal if the threat model ever requires them.

### 4. No handshake

When two peers open a TCP connection, the initiator immediately sends the first framed message. No version exchange, no capability negotiation, no auth round-trip.

**Why:** The version byte in the frame header implicitly handles protocol negotiation -- if either side sees a version it doesn't understand, it closes the connection. A proper handshake would require an additional round trip per sync cycle, which is wasted latency when the expected case is "both sides are running the same version."

**Consequence:** Protocol version bumps are breaking. Peers running version N cannot talk to peers running version N+1. This is acceptable given the embedding model -- smugglr and legion are co-deployed, and the version byte gives us a diagnostic when they drift.

### 5. Replay protection stays at the delta layer

`ReplayGuard` operates on delta sequence numbers inside `DeltaPacket` bodies. The frame layer does not duplicate this work.

**Why:** Frame-level replay protection would require a nonce window per connection, which conflicts with the stateless per-frame encryption decision. The delta layer already rejects duplicate sequences across both UDP and TCP paths, which is exactly the property we want. The AEAD envelope gives us per-frame integrity and confidentiality; `ReplayGuard` gives us freshness at the semantic layer where it matters.

### 6. Maximum frame size stays at 64 MB

Inherit the existing `read_framed` cap.

**Why:** 64 MB is already generous for sync payloads. `DeltaPacket` has its own chunking for oversized deltas (inherited from the UDP path) so nothing breaks when a single logical delta exceeds the frame cap. Raising this would only enable larger single-request memory use without a corresponding benefit.

### 7. Backpressure is tokio's natural write_all backpressure

`tokio::io::AsyncWriteExt::write_all` awaits socket readiness. No explicit window or credit protocol.

**Why:** One TCP connection per sync exchange, sequential frame exchange, short-lived connections. The complexity of explicit backpressure (windows, credits, pacing) buys nothing over what the kernel and tokio already do for us.

## Wire format

All multi-byte integers are big-endian.

### Plaintext frame (no key configured)

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+---------------+-------+-------+-------------------------------+
|                     total_length                             |
+---------------+-------+-------+-------------------------------+
|    version    |     flags     |                               |
+---------------+---------------+                               +
|                        payload (JSON)                         |
+---------------------------------------------------------------+
```

- `total_length` (u32): length of everything after this field (version + flags + payload). Hard cap: 64 MB.
- `version` (u8): frame format version. Initial value: `1`.
- `flags` (u8): see Flags table below. For plaintext frames, bit 0 (`ENCRYPTED`) is `0`.
- `payload`: UTF-8 JSON, length = `total_length - 2`.

### Encrypted frame (key configured)

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+---------------+-------+-------+-------------------------------+
|                     total_length                             |
+---------------+-------+-------+-------------------------------+
|    version    |     flags     |                               |
+---------------+---------------+                               +
|                          nonce (24 bytes)                     |
+                                                               +
|                                                               |
+                                                               +
|                                                               |
+                                                               +
|                                                               |
+                               +-------------------------------+
|                               |                               |
+-------------------------------+                               +
|                  ciphertext + Poly1305 tag                    |
+---------------------------------------------------------------+
```

- `total_length` (u32): length of everything after this field (version + flags + nonce + ciphertext + tag). Hard cap: 64 MB.
- `version` (u8): frame format version. Initial value: `1`.
- `flags` (u8): bit 0 (`ENCRYPTED`) is `1`.
- `nonce` (24 bytes): fresh random XChaCha20 nonce per frame.
- `ciphertext + tag`: AEAD output from `XChaCha20-Poly1305`. The plaintext, once decrypted, is UTF-8 JSON (same schema as the plaintext frame payload). The last 16 bytes are the Poly1305 tag.

### Flags

| Bit | Name         | Description                                                   |
|-----|--------------|---------------------------------------------------------------|
| 0   | `ENCRYPTED`  | Set if this frame contains an XChaCha20-Poly1305 envelope.    |
| 1-7 | reserved     | Must be 0 on send. Receivers MUST reject frames with any reserved bit set. |

## Connection lifecycle

1. **Open.** Initiator opens TCP to responder's `sync_port` (from the UDP announcement).
2. **Request.** Initiator sends one framed `SyncRequest` (encrypted if both sides have a key).
3. **Response.** Responder reads the frame, verifies version + flags, decrypts if needed, parses JSON, processes the request against local data, writes one framed `SyncResponse`.
4. **Close.** Both sides drop the connection.

No keep-alive. No reuse. Each sync exchange is a fresh TCP connection. This matches the existing plaintext implementation.

## State machine (receiver side, per connection)

```
AwaitingHeader
    |
    | read 4 bytes (total_length)
    v
AwaitingMetadata
    |
    | read 2 bytes (version, flags)
    | verify version == 1
    | verify reserved flags == 0
    v
BranchOnEncryption
    |
    +-- flags & ENCRYPTED == 0 --> AwaitingPayload (plaintext)
    |       |
    |       | read (total_length - 2) bytes
    |       v
    |   DeserializeJson
    |
    +-- flags & ENCRYPTED == 1 --> AwaitingNonce
            |
            | read 24 bytes
            v
        AwaitingCiphertext
            |
            | read (total_length - 2 - 24) bytes
            | decrypt via XChaCha20-Poly1305
            v
        DeserializeJson
```

Any read error, version mismatch, reserved flag bit set, decrypt failure, or JSON parse failure terminates the connection without further processing. No error frames are sent back -- TCP reset is the signal.

## Configuration invariants

- `BroadcastConfig.secret` is the single source of truth for the TCP encryption key. The same value is used for UDP encryption.
- If one side has a secret configured and the other does not, decryption fails and the connection is dropped. This is correct behavior.
- `BroadcastConfig.secret` currently has two `#[allow(dead_code)]` call sites (`secret` field, `encryption_key()` method). Both become live when this spec is implemented.

## Interop with legion

Legion embeds smugglr-core as a library dependency and drives `run_broadcast_once` from its broadcast-sync actor. The public surface legion consumes is already `pub` via `smugglr_core::broadcast::*`:

- `BroadcastConfig` (with `secret`)
- `run_broadcast_once` (the loop entry point)
- `ReplayGuard`
- The `DeltaPacket` type
- `handle_sync_connection` (the TCP handler)

This spec adds **no new public API surface**. The frame format change is internal to `broadcast.rs` -- the callers continue to hand typed `SyncRequest` / `SyncResponse` values to `write_framed` / `read_framed` and get typed results back. The only visible behavior change is that traffic on the wire is now encrypted when a secret is configured.

**Legion's integration cost:** zero. Legion already calls `run_broadcast_once` and already reads `BroadcastConfig` from its own config source. The same secret flows through.

## Implementation plan

Reference implementation lands in `crates/smugglr-core/src/broadcast.rs`. No new files.

### Step 1: frame format types

Add a private `FrameHeader` helper:

```rust
#[repr(u8)]
const FRAME_VERSION: u8 = 1;

bitflags::bitflags! {
    struct FrameFlags: u8 {
        const ENCRYPTED = 0b0000_0001;
    }
}
```

Or without the bitflags crate, just a `const ENCRYPTED_FLAG: u8 = 0x01;` and bit tests. Both are fine; choose whatever the rest of smugglr-core already uses. (Search shows no current bitflags usage, so prefer the const approach to avoid adding a dependency.)

### Step 2: modify `write_framed` to accept an optional key

```rust
async fn write_framed<T: Serialize>(
    stream: &mut OwnedWriteHalf,
    msg: &T,
    key: Option<&[u8; 32]>,
) -> Result<()>
```

- Serialize `msg` to JSON bytes.
- Compute `payload_bytes`: either the JSON bytes (plaintext) or `encrypt_packet(json, key)` (encrypted).
- Compute `total_length = 2 + payload_bytes.len()` (2 for version + flags).
- Write: `[total_length as u32 BE][FRAME_VERSION][flags][payload_bytes]`.

### Step 3: modify `read_framed` to accept an optional key

```rust
async fn read_framed<T: DeserializeOwned>(
    stream: &mut OwnedReadHalf,
    key: Option<&[u8; 32]>,
) -> Result<T>
```

- Read 4 bytes, decode `total_length`. Reject if > 64 MB.
- Read 2 bytes, verify `version == 1`. Reject on mismatch.
- Check `flags` against `key`:
  - `ENCRYPTED` set, `key` is `None` -> reject (no key to decrypt).
  - `ENCRYPTED` clear, `key` is `Some` -> reject (refusing to accept plaintext when encryption is configured).
  - Reserved bits set -> reject.
- Read `total_length - 2` bytes into buffer.
- If encrypted: call `decrypt_packet(&buf, key)` to get JSON bytes.
- `serde_json::from_slice` the JSON bytes into `T`.

### Step 4: remove `#[allow(dead_code)]` attributes

- `BroadcastConfig.secret`
- `BroadcastConfig::encryption_key`
- `encrypt_packet`
- `decrypt_packet`
- `maybe_encrypt` / `maybe_decrypt` -- keep if UDP path still uses them, otherwise delete.

### Step 5: update callers

- `handle_sync_connection` (responder): resolve the key from `broadcast_config.encryption_key()?` once at connection start, thread it through the `read_framed` / `write_framed` calls.
- `sync_with_peer` (initiator, exists elsewhere in `broadcast.rs`): same pattern.

### Step 6: tests

- **Plaintext round-trip.** `write_framed(None)` then `read_framed(None)` recovers the original `SyncRequest`.
- **Encrypted round-trip.** `write_framed(Some(key))` then `read_framed(Some(key))` recovers the original `SyncRequest`.
- **Key mismatch.** `write_framed(Some(key_a))` then `read_framed(Some(key_b))` fails with an auth error.
- **Plaintext rejected in encrypted mode.** `write_framed(None)` then `read_framed(Some(key))` rejects at the flags check.
- **Encrypted rejected in plaintext mode.** `write_framed(Some(key))` then `read_framed(None)` rejects at the flags check.
- **Version mismatch.** Craft a frame with `version = 0` -> rejected.
- **Reserved flag bits.** Craft a frame with `flags = 0x02` -> rejected.
- **Oversize frame.** Craft a frame with `total_length > 64 MB` -> rejected.
- **Truncated frame.** Close the stream mid-read -> `read_framed` returns an error.
- **Tampered ciphertext.** Flip a byte in the ciphertext portion -> AEAD tag verification fails.

## Failure modes

| Scenario                          | Detection                          | Behavior              |
|-----------------------------------|------------------------------------|-----------------------|
| Peer on different protocol version| Version byte mismatch              | Drop connection       |
| Peer using different secret       | AEAD tag verification fails        | Drop connection       |
| One side has secret, other doesn't| Flags/key mismatch check           | Drop connection       |
| Tampered frame in transit         | AEAD tag verification fails        | Drop connection       |
| Replayed delta at semantic layer  | `ReplayGuard` rejects sequence     | Drop delta, keep conn |
| Oversize frame                    | `total_length > 64 MB` check       | Drop connection       |
| Reserved flag bits set            | Flags check                        | Drop connection       |

All connection-level failures are silent (TCP reset). Peers do not get detailed error information from the wire -- the assumption is that misconfigured peers are a control-plane problem, not a data-plane problem.

## Open questions

None currently block implementation. The decisions above are all load-bearing but defensible. If legion has a different read on any of them, this spec gets revised before the reference implementation lands.

Specifically I want legion's take on:

1. **Per-frame random nonce.** Does legion's broadcast-sync actor have any state that would benefit from counter nonces? My read is no, and the embedding decision should not resurrect complexity that smugglr dropped.
2. **No handshake.** Does legion need to do capability negotiation for its own reasons (e.g., feature flags in its MCP surface)? If so, the spec needs a handshake frame at the top and I would prefer to add one now rather than retrofit later.
3. **Delta-layer replay protection is sufficient.** Confirming that legion will not add its own frame-level replay window.

## Appendix: rejected alternatives

**Session-level key agreement.** Noise Protocol or similar. Rejected: overkill for the PSK-on-LAN threat model, and embedding a handshake framework is a large dependency for a small benefit.

**Per-frame counter nonces.** Rejected: requires synchronized state, complicates reconnects, saves zero bytes in the 192-bit nonce field.

**TLS.** Rejected: the whole point of the existing envelope is that it works without certificate infrastructure on the LAN. TLS brings cert management, cipher negotiation, and a handshake round trip -- all costs we explicitly opted out of.

**Length prefix at u16 (64 KB cap).** Rejected: delta packets for large tables can exceed 64 KB even after chunking, and the existing 64 MB cap already handles every realistic case.

**Message type discriminator in the header.** Rejected: `SyncRequest` and `SyncResponse` are distinguished by exchange position (first frame from initiator is always a request, reply from responder is always a response). No dispatch layer needed.

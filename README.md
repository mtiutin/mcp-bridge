# mcp-bridge

A tiny Rust **stdio ↔ HTTP transport bridge** for the [Model Context Protocol](https://modelcontextprotocol.io). It is a drop-in replacement for `npx mcp-remote`: it speaks JSON-RPC over stdin/stdout to a host agent, and proxies that traffic to a remote MCP server over **streamable-HTTP** (2025-03-26 spec) or **legacy SSE**.

It exists because the Node version was eating the host alive.

---

## The problem we hit with Hermes

Our agent stack runs [**Hermes Agent**](https://github.com/NousResearch/hermes-agent) (by [Nous Research](https://github.com/NousResearch)) on a single 7.6 GB box. Hermes connects to ~16 MCP servers — ClickHouse, Postgres, Metabase, TRON tooling, a papers corpus, several remote HTTP MCPs, and so on.

Hermes can talk to a streamable-HTTP MCP server directly (`mcp_servers.<name>.url`). But for any server that only speaks the **legacy SSE** transport, the agent needs a stdio bridge subprocess in between. The canonical bridge is `npx mcp-remote`, a Node program.

That bridge turned out to be the single most expensive thing on the box:

- **~95 MB resident per instance.** Six legacy-SSE backends meant **~570 MB** just in bridge processes — for what is, functionally, a byte shuffler between a pipe and an HTTP socket.
- **The gateway kept getting OOM-killed.** Two things compounded: the gateway unit shipped with `oom_score_adj=100`, marking it as the kernel's *preferred* victim, and the bridge processes left no headroom. Under any memory pressure, the kernel killed the gateway — taking down every channel (WhatsApp, Signal) at once.

We fixed the operational side two ways:

1. **Moved most MCPs off bridges entirely.** Where the upstream supports streamable-HTTP, Hermes connects directly via `url:` + `headers:` — no subprocess at all. (We even rebuilt `postgres-mcp` from HEAD to get native streamable-HTTP and retired its bridge.)
2. **Replaced the remaining bridges with this binary.** For the backends that genuinely only speak legacy SSE, `mcp-bridge` does the same job as `npx mcp-remote` at **~2 MB resident** instead of ~95 MB.

We also set `OOMScoreAdjust=-500` on the gateway unit so it's no longer the kernel's first pick. Net effect: the bridge layer went from ~570 MB to a couple of MB, and the OOM kills stopped.

---

## What it does

```
host agent  ──stdin/stdout (JSON-RPC lines)──▶  mcp-bridge  ──HTTP──▶  MCP server
```

- **Streamable-HTTP** (default): POSTs each stdin line to the server; relays JSON or `text/event-stream` responses back to stdout. Tracks `Mcp-Session-Id`. Opens a background `GET` subscription for server-initiated messages, with exponential-backoff reconnect (and quietly gives up if the server answers `405`/`404`, meaning it doesn't support GET subscriptions).
- **Legacy SSE**: connects `GET /sse`, waits for the server's `endpoint` event to learn the POST URL, then forwards stdin lines there. Reconnects with backoff if the stream drops.
- **Transport auto-detection**: a URL path ending in `/sse` (or containing `/sse/`) is treated as legacy SSE; everything else is streamable-HTTP. Override with `--transport`.
- **HTTPS by default**: plain `http://` is rejected unless the host is loopback or you pass `--allow-http`.

It is ~400 lines of Rust, built for size (`opt-level = "z"`, LTO, stripped, `panic = "abort"`), producing a ~2 MB static binary.

---

## Usage

```
mcp-bridge <URL> [--header "K: V"]... [--allow-http] [--transport http|sse|auto] [-v]
```

| Flag | Default | Meaning |
|---|---|---|
| `<URL>` | — | MCP server URL (`…/mcp` for streamable-HTTP, `…/sse` for legacy SSE) |
| `--header "K: V"` | — | Add an HTTP header; repeatable (e.g. auth tokens) |
| `--allow-http` | off | Permit non-loopback plain `http://` URLs |
| `--transport` | `auto` | Force `http`, `sse`, or auto-detect from the path |
| `-v`, `--verbose` | off | Diagnostic logging to **stderr** (stdout stays pure JSON-RPC) |

Examples:

```bash
# Legacy SSE backend on localhost (auto-detected from the /sse suffix)
mcp-bridge http://127.0.0.1:9801/sse

# Remote streamable-HTTP MCP with an auth header
mcp-bridge https://mcp.example.com/mcp --header "X-API-Key: $TOKEN"

# Force legacy SSE and watch the handshake
mcp-bridge https://mcp.example.com/stream --transport sse -v
```

> **Note:** diagnostics go to stderr only. stdout carries the JSON-RPC stream and must never be polluted, or the host agent's parser will choke.

---

## Build

Standard Cargo project, no build scripts.

### Native (build where you'll run it)

```bash
cargo build --release
# binary at target/release/mcp-bridge
```

### Cross-compile from macOS to a Linux server (how we ship it)

We develop on macOS and run on Ubuntu x86_64. A fully static musl binary cross-builds cleanly with [`cargo-zigbuild`](https://github.com/rust-cross/cargo-zigbuild):

```bash
# one-time toolchain setup
brew install zig
cargo install cargo-zigbuild
rustup target add x86_64-unknown-linux-musl

# build the static binary
cargo zigbuild --release --target x86_64-unknown-linux-musl
# binary at target/x86_64-unknown-linux-musl/release/mcp-bridge (~2 MB, no libc dependency)
```

### Deploy

```bash
scp -i ~/.ssh/your-key.pem \
    target/x86_64-unknown-linux-musl/release/mcp-bridge \
    root@<server>:/usr/local/bin/mcp-bridge
ssh -i ~/.ssh/your-key.pem root@<server> 'chmod +x /usr/local/bin/mcp-bridge'
```

---

## Wiring it into Hermes

> Built and tested against [Hermes Agent](https://github.com/NousResearch/hermes-agent). The CLI mirrors `npx mcp-remote`, so it also drops into any other MCP host that launches a stdio bridge subprocess (Claude Desktop, Cursor, etc.).

In `~/.hermes/config.yaml`, point the MCP entry at the binary and pass the upstream URL as an argument:

```yaml
mcp_servers:
  mcp-foo:
    command: /usr/local/bin/mcp-bridge
    args: [http://127.0.0.1:9801/sse]
```

Then `hermes gateway restart` so the messaging path picks up the change. Verify with `hermes mcp test mcp-foo`.

---

## Guidelines — when (and when not) to use this

The bridge is a fallback, not the default. Prefer the lightest transport the upstream supports.

1. **Direct `url:` first.** If the MCP server speaks streamable-HTTP, wire it into Hermes with `mcp_servers.<name>: { url: <URL>, headers: {...} }`. No subprocess, no bridge — Hermes connects natively. This covers local FastMCP/supergateway servers and remote streamable-HTTP MCPs alike.

2. **Reach for `mcp-bridge` only on legacy SSE.** The tell: a direct `url:` config fails with **`405 Method Not Allowed`** (Hermes tries `POST /sse`; a legacy server only accepts `GET /sse` and replies with an `endpoint` event). That's the one case the native client can't handle. Switch the entry to the `command:`/`args:` form above.

3. **Never use `npx mcp-remote` / `command: npx`.** That's the ~95 MB-per-instance pattern this project was built to retire. `mcp-bridge` is a drop-in replacement — same CLI shape, ~2 MB.

4. **Keep stdout clean.** Anything the bridge needs to say goes to stderr (`-v`). stdout is the JSON-RPC channel and nothing else.

5. **Don't relax HTTPS without reason.** `--allow-http` is for trusted loopback/LAN backends only; leave it off for anything reachable over the network.

6. **One bridge process per backend.** Like the rest of the local-MCP layout, treat each bridge as a long-lived singleton the agent connects to, not something to spawn per session.

---

## Layout

```
src/main.rs    # the whole bridge (~400 lines): arg parsing, streamable-HTTP, legacy SSE
Cargo.toml     # deps + size-optimized release profile
```

Dependencies: `tokio`, `reqwest` (rustls, no OpenSSL), `eventsource-stream`, `clap`, `serde_json`, `url`, `anyhow`.

---

## Credits & related

- [**Hermes Agent**](https://github.com/nousresearch/hermes-agent) by [Nous Research](https://github.com/NousResearch) — the agent runtime this bridge was built for.
- [**mcp-remote**](https://www.npmjs.com/package/mcp-remote) — the Node tool whose CLI this mirrors and whose memory footprint motivated the rewrite.
- [**Model Context Protocol**](https://modelcontextprotocol.io) — transport spec ([2025-03-26](https://modelcontextprotocol.io/specification/2025-03-26/basic/transports)).

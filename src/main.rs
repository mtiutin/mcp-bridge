//! mcp-bridge — stdio↔HTTP MCP transport bridge.
//!
//! Drop-in replacement for `npx mcp-remote <URL> [--header K:V]... [--allow-http]`,
//! used by Hermes (and similar agents) to talk to MCP servers that don't speak
//! stdio. Target memory footprint: ~10 MB resident (vs ~85 MB for the Node version).
//!
//! Supports two MCP transports:
//!   - **streamable-HTTP** (post-2025 spec; default)
//!   - **legacy SSE** (pre-streamable; auto-detected from `/sse` path suffix)
//!
//! See <https://modelcontextprotocol.io/specification/2025-03-26/basic/transports>.

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, ACCEPT, CONTENT_TYPE};
use reqwest::{Client, StatusCode};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, Mutex};

const MCP_SESSION: &str = "Mcp-Session-Id";
const ACCEPT_BOTH: &str = "application/json, text/event-stream";
const RECONNECT_BASE_MS: u64 = 500;
const RECONNECT_MAX_MS: u64 = 30_000;

#[derive(Parser, Debug)]
#[command(name = "mcp-bridge", version, about = "Stdio↔HTTP MCP transport bridge")]
struct Args {
    /// MCP server URL (streamable-HTTP /mcp or legacy /sse)
    url: String,

    /// HTTP header in "Key: Value" form (repeatable)
    #[arg(long)]
    header: Vec<String>,

    /// Allow plain http:// URLs (default: only https:// or 127.0.0.1)
    #[arg(long)]
    allow_http: bool,

    /// Force transport: "http" (streamable), "sse" (legacy), or "auto"
    #[arg(long, default_value = "auto")]
    transport: String,

    /// Print diagnostic messages to stderr
    #[arg(long, short = 'v')]
    verbose: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    rt.block_on(async_main(args))
}

async fn async_main(args: Args) -> Result<()> {
    let parsed = url::Url::parse(&args.url).context("invalid URL")?;
    let is_localhost = matches!(
        parsed.host_str(),
        Some("127.0.0.1") | Some("localhost") | Some("::1")
    );
    if parsed.scheme() == "http" && !is_localhost && !args.allow_http {
        bail!("http:// not allowed (pass --allow-http for non-localhost), use https://");
    }

    let mut headers = HeaderMap::new();
    for h in &args.header {
        let (k, v) = h
            .split_once(':')
            .ok_or_else(|| anyhow!("header must be K:V form, got: {h}"))?;
        let name = HeaderName::from_bytes(k.trim().as_bytes())
            .with_context(|| format!("bad header name: {k}"))?;
        let value = HeaderValue::from_str(v.trim())
            .with_context(|| format!("bad header value for {k}"))?;
        headers.insert(name, value);
    }

    let transport = if args.transport == "auto" {
        if parsed.path().ends_with("/sse") || parsed.path().contains("/sse/") {
            "sse"
        } else {
            "http"
        }
    } else {
        args.transport.as_str()
    };
    eprintln_v(
        args.verbose,
        &format!("mcp-bridge transport={transport} url={}", args.url),
    );

    let client = Client::builder()
        .user_agent(concat!("mcp-bridge/", env!("CARGO_PKG_VERSION")))
        .pool_idle_timeout(std::time::Duration::from_secs(60))
        .timeout(std::time::Duration::from_secs(900))
        .build()?;

    match transport {
        "http" => streamable_http(client, args.url, headers, args.verbose).await,
        "sse" => legacy_sse(client, args.url, headers, args.verbose).await,
        other => bail!("unknown transport: {other}"),
    }
}

// ---------------------------------------------------------------------------
// Streamable-HTTP (modern, 2025-03-26 spec)
// ---------------------------------------------------------------------------

async fn streamable_http(
    client: Client,
    url: String,
    headers: HeaderMap,
    verbose: bool,
) -> Result<()> {
    let session: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let (stdout_tx, mut stdout_rx) = mpsc::channel::<String>(64);

    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(line) = stdout_rx.recv().await {
            if stdout.write_all(line.as_bytes()).await.is_err() {
                break;
            }
            if !line.ends_with('\n') {
                if stdout.write_all(b"\n").await.is_err() {
                    break;
                }
            }
            let _ = stdout.flush().await;
        }
    });

    let get_client = client.clone();
    let get_url = url.clone();
    let get_headers = headers.clone();
    let get_session = session.clone();
    let get_tx = stdout_tx.clone();
    let get_task = tokio::spawn(async move {
        let mut backoff_ms = RECONNECT_BASE_MS;
        loop {
            let mut req = get_client.get(&get_url).headers(get_headers.clone());
            req = req.header(ACCEPT, "text/event-stream");
            if let Some(sid) = get_session.lock().await.clone() {
                req = req.header(MCP_SESSION, sid);
            }
            match req.send().await {
                Ok(resp) => {
                    if resp.status() == StatusCode::METHOD_NOT_ALLOWED
                        || resp.status() == StatusCode::NOT_FOUND
                    {
                        eprintln_v(
                            verbose,
                            "server does not support GET subscription; not retrying",
                        );
                        break;
                    }
                    if !resp.status().is_success() {
                        eprintln_v(
                            verbose,
                            &format!("GET subscription status={}; reconnecting", resp.status()),
                        );
                    } else {
                        backoff_ms = RECONNECT_BASE_MS;
                        let mut stream = resp.bytes_stream().eventsource();
                        while let Some(ev) = stream.next().await {
                            match ev {
                                Ok(event) => {
                                    if event.data.is_empty() {
                                        continue;
                                    }
                                    if get_tx.send(event.data).await.is_err() {
                                        return;
                                    }
                                }
                                Err(e) => {
                                    eprintln_v(verbose, &format!("GET sse error: {e}"));
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(e) => eprintln_v(verbose, &format!("GET subscribe failed: {e}")),
            }
            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
            backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
        }
    });

    let mut stdin = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = stdin.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let body = line.clone();

        let mut req = client.post(&url).headers(headers.clone());
        req = req
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, ACCEPT_BOTH)
            .body(body);
        if let Some(sid) = session.lock().await.clone() {
            req = req.header(MCP_SESSION, sid);
        }

        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                eprintln_v(verbose, &format!("POST failed: {e}"));
                continue;
            }
        };

        if let Some(sid) = resp.headers().get(MCP_SESSION) {
            if let Ok(s) = sid.to_str() {
                *session.lock().await = Some(s.to_string());
            }
        }

        let status = resp.status();
        let ct = resp
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_ascii_lowercase();

        if status == StatusCode::ACCEPTED {
            continue;
        }
        if !status.is_success() {
            let txt = resp.text().await.unwrap_or_default();
            eprintln_v(verbose, &format!("HTTP {}: {}", status, txt));
            continue;
        }

        if ct.starts_with("application/json") {
            let body = resp.text().await?;
            if !body.trim().is_empty() {
                stdout_tx.send(body).await.ok();
            }
        } else if ct.starts_with("text/event-stream") {
            let mut stream = resp.bytes_stream().eventsource();
            while let Some(ev) = stream.next().await {
                match ev {
                    Ok(event) => {
                        if event.data.is_empty() {
                            continue;
                        }
                        if stdout_tx.send(event.data).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln_v(verbose, &format!("response sse error: {e}"));
                        break;
                    }
                }
            }
        } else {
            let body = resp.text().await.unwrap_or_default();
            if !body.trim().is_empty() {
                stdout_tx.send(body).await.ok();
            }
        }
    }

    drop(stdout_tx);
    let _ = writer.await;
    get_task.abort();
    Ok(())
}

// ---------------------------------------------------------------------------
// Legacy SSE (pre-streamable spec)
// ---------------------------------------------------------------------------

async fn legacy_sse(
    client: Client,
    url: String,
    headers: HeaderMap,
    verbose: bool,
) -> Result<()> {
    let (stdout_tx, mut stdout_rx) = mpsc::channel::<String>(64);
    let (endpoint_tx, mut endpoint_rx) = mpsc::channel::<String>(1);

    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(line) = stdout_rx.recv().await {
            if stdout.write_all(line.as_bytes()).await.is_err() {
                break;
            }
            if !line.ends_with('\n') {
                if stdout.write_all(b"\n").await.is_err() {
                    break;
                }
            }
            let _ = stdout.flush().await;
        }
    });

    let sse_url = url.clone();
    let sse_headers = headers.clone();
    let sse_client = client.clone();
    let sse_tx = stdout_tx.clone();
    let sse_task = tokio::spawn(async move {
        let mut backoff_ms = RECONNECT_BASE_MS;
        let mut endpoint_sent = false;
        loop {
            let mut req = sse_client.get(&sse_url).headers(sse_headers.clone());
            req = req.header(ACCEPT, "text/event-stream");
            match req.send().await {
                Ok(resp) if resp.status().is_success() => {
                    backoff_ms = RECONNECT_BASE_MS;
                    let mut stream = resp.bytes_stream().eventsource();
                    while let Some(ev) = stream.next().await {
                        match ev {
                            Ok(event) => {
                                if event.event == "endpoint" {
                                    let post_url = resolve_endpoint(&sse_url, &event.data);
                                    eprintln_v(verbose, &format!("legacy-sse endpoint={post_url}"));
                                    if !endpoint_sent {
                                        if endpoint_tx.send(post_url).await.is_err() {
                                            return;
                                        }
                                        endpoint_sent = true;
                                    }
                                } else if event.data.is_empty() {
                                    continue;
                                } else if sse_tx.send(event.data).await.is_err() {
                                    return;
                                }
                            }
                            Err(e) => {
                                eprintln_v(verbose, &format!("sse parse error: {e}"));
                                break;
                            }
                        }
                    }
                }
                Ok(resp) => {
                    eprintln_v(verbose, &format!("sse status={}; reconnect", resp.status()));
                }
                Err(e) => eprintln_v(verbose, &format!("sse connect failed: {e}")),
            }
            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
            backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
        }
    });

    let endpoint = endpoint_rx
        .recv()
        .await
        .ok_or_else(|| anyhow!("sse channel closed before endpoint received"))?;

    let mut stdin = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = stdin.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let mut req = client.post(&endpoint).headers(headers.clone());
        req = req.header(CONTENT_TYPE, "application/json").body(line);
        match req.send().await {
            Ok(resp) if !resp.status().is_success() && resp.status() != StatusCode::ACCEPTED => {
                let txt = resp.text().await.unwrap_or_default();
                eprintln_v(verbose, &format!("legacy-sse POST status err: {txt}"));
            }
            Ok(_) => {}
            Err(e) => eprintln_v(verbose, &format!("legacy-sse POST send err: {e}")),
        }
    }

    drop(stdout_tx);
    let _ = writer.await;
    sse_task.abort();
    Ok(())
}

fn resolve_endpoint(base: &str, endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        return endpoint.to_string();
    }
    match url::Url::parse(base) {
        Ok(b) => b
            .join(endpoint)
            .map(|u| u.to_string())
            .unwrap_or_else(|_| endpoint.to_string()),
        Err(_) => endpoint.to_string(),
    }
}

fn eprintln_v(verbose: bool, msg: &str) {
    if verbose {
        eprintln!("[mcp-bridge] {msg}");
    }
}

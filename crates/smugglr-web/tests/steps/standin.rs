//! The stand-in endpoint: a local HTTP server that answers the place
//! operations from rows the test sets in a SQLite file, records every
//! request it receives, and can drop the connection. It
//! also serves the page, @smugglr/web, the engine and SQLite to the tabs,
//! from one origin, cross-origin isolated as SQLite's OPFS needs.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use serde_json::{Value as Json, json};
use smugglr_core::change::{Change, Op};
use smugglr_core::hex::{hex, unhex};
use smugglr_place::{LocalFile, Place, Table};

/// One request the endpoint received.
#[derive(Debug, Clone)]
pub(crate) struct Logged {
    /// Its body, as sent.
    pub(crate) body: String,
    /// The `x-tab` header the tab's app sent with it.
    pub(crate) tab: String,
}

#[derive(Debug, Default)]
struct State {
    log: Vec<Logged>,
    /// Drops the connection on this `rows` request, counted from 1.
    drop_rows: Option<usize>,
    rows_seen: usize,
}

/// The stand-in endpoint and the origin the tabs load from.
#[derive(Debug)]
pub(crate) struct StandIn {
    /// `http://127.0.0.1:<port>`.
    pub(crate) origin: String,
    /// The SQLite file the endpoint answers from.
    pub(crate) db: PathBuf,
    state: Arc<Mutex<State>>,
}

impl StandIn {
    /// Starts the stand-in on a free port, answering from `db`.
    pub(crate) fn start(db: PathBuf) -> Result<Self, String> {
        let listener = TcpListener::bind("127.0.0.1:0").map_err(|err| err.to_string())?;
        let origin = format!(
            "http://127.0.0.1:{}",
            listener.local_addr().map_err(|err| err.to_string())?.port()
        );
        let state = Arc::new(Mutex::new(State::default()));
        let (shared, file) = (Arc::clone(&state), db.clone());
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let (state, file) = (Arc::clone(&shared), file.clone());
                std::thread::spawn(move || {
                    let _ = serve(stream, &state, &file);
                });
            }
        });
        Ok(Self { origin, db, state })
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Every request recorded so far.
    pub(crate) fn log(&self) -> Vec<Logged> {
        self.state().log.clone()
    }

    /// Drops the connection on the `n`th `rows` request from now; `None`
    /// keeps every connection whole.
    pub(crate) fn drop_rows(&self, n: Option<usize>) {
        let mut state = self.state();
        state.drop_rows = n;
        state.rows_seen = 0;
    }
}

const ROOT: &str = env!("CARGO_MANIFEST_DIR");

/// Where each path the tabs load is served from.
fn served(path: &str) -> Option<PathBuf> {
    let web = Path::new(ROOT).join("../../packages/web");
    let routes = [
        ("/web/engine/", Path::new(ROOT).join("pkg")),
        (
            "/web/sqlite/",
            web.join("node_modules/@sqlite.org/sqlite-wasm/dist"),
        ),
        ("/web/", web.join("src")),
    ];
    let (prefix, folder) = routes
        .into_iter()
        .find(|(prefix, _)| path.starts_with(prefix))?;
    let rest = &path[prefix.len()..];
    (!rest.contains("..")).then(|| folder.join(rest))
}

fn kind(path: &Path) -> &'static str {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("js" | "mjs") => "text/javascript",
        Some("wasm") => "application/wasm",
        _ => "application/octet-stream",
    }
}

fn respond(stream: &mut TcpStream, status: &str, kind: &str, body: &[u8]) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\n\
         Cross-Origin-Opener-Policy: same-origin\r\nCross-Origin-Embedder-Policy: require-corp\r\n\
         Cross-Origin-Resource-Policy: same-origin\r\nCache-Control: no-store\r\n\
         Connection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    stream.flush()
}

fn serve(mut stream: TcpStream, state: &Mutex<State>, db: &Path) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let mut parts = line.split_whitespace();
    let (method, path) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
    // A query string names no file.
    let path = path.split('?').next().unwrap_or(path);
    let mut headers = BTreeMap::new();
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header)? == 0 || header.trim().is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
        }
    }
    let length = headers
        .get("content-length")
        .and_then(|length| length.parse().ok())
        .unwrap_or(0);
    let mut body = vec![0; length];
    reader.read_exact(&mut body)?;
    match (method, path) {
        ("GET", "/") => respond(
            &mut stream,
            "200 OK",
            "text/html",
            b"<!doctype html><title>smugglr</title>",
        ),
        ("GET", _) => {
            match served(path).and_then(|file| Some((std::fs::read(&file).ok()?, file))) {
                Some((bytes, file)) => respond(&mut stream, "200 OK", kind(&file), &bytes),
                None => respond(&mut stream, "404 Not Found", "text/plain", b"missing"),
            }
        }
        ("POST", "/endpoint") => {
            let body = String::from_utf8_lossy(&body).into_owned();
            let request: Json = serde_json::from_str(&body).unwrap_or(Json::Null);
            let op = request["op"].as_str().unwrap_or_default().to_owned();
            let drop = {
                let mut state = state.lock().unwrap_or_else(PoisonError::into_inner);
                state.log.push(Logged {
                    body,
                    tab: headers.get("x-tab").cloned().unwrap_or_default(),
                });
                if op == "rows" {
                    state.rows_seen += 1;
                }
                op == "rows" && state.drop_rows == Some(state.rows_seen)
            };
            if drop {
                // The connection drops with no answer.
                return stream.shutdown(std::net::Shutdown::Both);
            }
            match answer(db, &request) {
                Ok(json) => respond(
                    &mut stream,
                    "200 OK",
                    "application/json",
                    json.to_string().as_bytes(),
                ),
                Err(err) => respond(&mut stream, "400 Bad Request", "text/plain", err.as_bytes()),
            }
        }
        _ => respond(&mut stream, "404 Not Found", "text/plain", b"missing"),
    }
}

fn texts(json: &Json) -> Result<Vec<String>, String> {
    json.as_array()
        .and_then(|items| {
            items
                .iter()
                .map(|item| item.as_str().map(str::to_owned))
                .collect()
        })
        .ok_or_else(|| format!("{json} is not a list of text"))
}

fn hash(json: &Json) -> Result<Option<[u8; 16]>, String> {
    match json {
        Json::Null => Ok(None),
        json => json
            .as_str()
            .and_then(unhex)
            .and_then(|bytes| bytes.try_into().ok())
            .map(Some)
            .ok_or_else(|| format!("{json} is not a row hash")),
    }
}

/// Answers one place operation from the endpoint's SQLite file.
fn answer(db: &Path, request: &Json) -> Result<Json, String> {
    let mut place = LocalFile::open(db).map_err(|err| err.to_string())?;
    let tables = place.tables().map_err(|err| err.to_string())?;
    let table = || -> Result<&Table, String> {
        let name = request["table"].as_str().ok_or("no table named")?;
        tables
            .iter()
            .find(|table| table.name == name)
            .ok_or_else(|| format!("no table {name}"))
    };
    let failed = |err: smugglr_place::Error| err.to_string();
    Ok(match request["op"].as_str() {
        Some("tables") => json!(
            tables
                .iter()
                .map(|table| json!({
                    "name": table.name,
                    "columns": table.columns,
                    "types": table.types,
                    "key": table.key,
                    "self_keyed": table.self_keyed,
                }))
                .collect::<Vec<_>>()
        ),
        Some("hashes") => json!(
            place
                .hashes(table()?)
                .map_err(failed)?
                .into_iter()
                .map(|(key, hash)| json!([key, hex(&hash)]))
                .collect::<Vec<_>>()
        ),
        Some("weights") => json!(
            place
                .weights(table()?)
                .map_err(failed)?
                .into_iter()
                .map(|(key, weight)| json!([key, weight]))
                .collect::<Vec<_>>()
        ),
        Some("rows") => {
            let keys = request["keys"]
                .as_array()
                .ok_or("no keys")?
                .iter()
                .map(texts)
                .collect::<Result<Vec<_>, String>>()?;
            json!(place.rows(table()?, &keys).map_err(failed)?)
        }
        Some("write") => {
            let table = table()?;
            let changes = request["changes"]
                .as_array()
                .ok_or("no changes")?
                .iter()
                .map(|change| {
                    Ok(Change {
                        op: match change["op"].as_str() {
                            Some("up") => Op::Up,
                            Some("down") => Op::Down,
                            Some("upsert") => Op::Upsert,
                            _ => return Err(format!("{change} has no op")),
                        },
                        table: table.name.clone(),
                        key: texts(&change["key"])?,
                        from: hash(&change["from"])?,
                        to: hash(&change["to"])?,
                        values: texts(&change["values"])?,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            json!(place.write(table, &changes).map_err(failed)?)
        }
        _ => return Err(format!("{request} names no place operation")),
    })
}

//! Tabs of one origin in headless Chromium, driven through
//! `packages/web/test/driver.mjs`, and the engine built for them once.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, ErrorKind, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use serde_json::{Value as Json, json};

/// The most browsers the scenarios run at once.
const AT_ONCE: usize = 3;

static RUNNING: (Mutex<usize>, Condvar) = (Mutex::new(0), Condvar::new());

/// Builds smugglr-web with wasm-pack into `crates/smugglr-web/pkg`, once.
fn built() -> Result<(), String> {
    static BUILT: OnceLock<Result<(), String>> = OnceLock::new();
    BUILT
        .get_or_init(|| {
            let web = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../packages/web");
            let pnpm = if cfg!(windows) { "pnpm.cmd" } else { "pnpm" };
            let output = Command::new(pnpm)
                .current_dir(&web)
                .args(["run", "build:wasm"])
                .env(
                    "CARGO_TARGET_DIR",
                    Path::new(env!("CARGO_TARGET_TMPDIR")).join("smugglr-web"),
                )
                .output()
                .map_err(|err| {
                    if err.kind() == ErrorKind::NotFound {
                        "pnpm is missing: install pnpm and wasm-pack, then run `pnpm install` \
                         at the repo root"
                            .to_owned()
                    } else {
                        format!("failed to start pnpm: {err}")
                    }
                })?;
            if output.status.success() {
                Ok(())
            } else {
                Err(format!(
                    "smugglr-web did not build:\n{}",
                    String::from_utf8_lossy(&output.stderr)
                ))
            }
        })
        .clone()
}

type Answers = Arc<(Mutex<HashMap<u64, Json>>, Condvar)>;

/// Headless Chromium, with tabs of one origin.
#[derive(Debug)]
pub(crate) struct Browser {
    child: Child,
    stdin: Mutex<ChildStdin>,
    answers: Answers,
    next: AtomicU64,
}

impl Browser {
    /// Starts the browser for tabs of `origin`, once a place among the
    /// scenarios' browsers is free.
    pub(crate) fn start(origin: &str) -> Result<Self, String> {
        built()?;
        {
            let (running, freed) = &RUNNING;
            let mut running = running.lock().unwrap_or_else(PoisonError::into_inner);
            while *running >= AT_ONCE {
                running = freed.wait(running).unwrap_or_else(PoisonError::into_inner);
            }
            *running += 1;
        }
        let web = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../packages/web");
        let mut child = Command::new("node")
            .current_dir(&web)
            .args(["test/driver.mjs", origin])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|err| {
                Self::freed();
                format!(
                    "failed to start node ({err}); run `pnpm exec playwright install \
                     chromium-headless-shell` in packages/web"
                )
            })?;
        let (Some(stdin), Some(stdout)) = (child.stdin.take(), child.stdout.take()) else {
            Self::freed();
            return Err("the driver has no stdin or stdout".to_owned());
        };
        let answers: Answers = Arc::default();
        let heard = Arc::clone(&answers);
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                let Ok(answer) = serde_json::from_str::<Json>(&line) else {
                    continue;
                };
                let Some(id) = answer["id"].as_u64() else {
                    continue;
                };
                let (map, arrived) = &*heard;
                map.lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(id, answer);
                arrived.notify_all();
            }
        });
        Ok(Self {
            child,
            stdin: Mutex::new(stdin),
            answers,
            next: AtomicU64::new(0),
        })
    }

    fn freed() {
        let (running, freed) = &RUNNING;
        *running.lock().unwrap_or_else(PoisonError::into_inner) -= 1;
        freed.notify_all();
    }

    /// Sends a command; returns its id.
    pub(crate) fn send(&self, mut command: Json) -> Result<u64, String> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        command["id"] = json!(id);
        let mut stdin = self.stdin.lock().unwrap_or_else(PoisonError::into_inner);
        writeln!(stdin, "{command}")
            .and_then(|()| stdin.flush())
            .map_err(|err| format!("the driver stopped: {err}"))?;
        Ok(id)
    }

    /// Waits for the answer to command `id`.
    pub(crate) fn answer(&self, id: u64) -> Result<Json, String> {
        let (map, arrived) = &*self.answers;
        let mut map = map.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            if let Some(answer) = map.remove(&id) {
                return if answer["ok"] == json!(true) {
                    Ok(answer["value"].clone())
                } else {
                    Err(answer["error"].as_str().unwrap_or("failed").to_owned())
                };
            }
            let (next, timeout) = arrived
                .wait_timeout(map, Duration::from_secs(180))
                .unwrap_or_else(PoisonError::into_inner);
            if timeout.timed_out() {
                return Err(format!("the driver gave no answer to command {id}"));
            }
            map = next;
        }
    }

    /// Evaluates `js`, an expression, in tab `tab`, and returns its value.
    pub(crate) fn eval(&self, tab: usize, js: &str) -> Result<Json, String> {
        let id = self.send(json!({"op": "eval", "tab": tab, "js": js}))?;
        self.answer(id)
    }

    /// Closes tab `tab`.
    pub(crate) fn close(&self, tab: usize) -> Result<(), String> {
        let id = self.send(json!({"op": "close", "tab": tab}))?;
        self.answer(id).map(drop)
    }

    /// Clears the origin's data.
    pub(crate) fn clear(&self) -> Result<(), String> {
        let id = self.send(json!({"op": "clear"}))?;
        self.answer(id).map(drop)
    }
}

impl Drop for Browser {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        Self::freed();
    }
}

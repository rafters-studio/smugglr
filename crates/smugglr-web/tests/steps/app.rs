//! An app that installed @smugglr/web from its packed package: the
//! package built for release by its prepack, as an app gets it.

use std::path::{Path, PathBuf};

/// Packs @smugglr/web with `pnpm pack`, once; the tarball's path.
fn packed() -> Result<PathBuf, String> {
    static PACKED: std::sync::OnceLock<Result<PathBuf, String>> = std::sync::OnceLock::new();
    PACKED
        .get_or_init(|| {
            let tmp = Path::new(env!("CARGO_TARGET_TMPDIR"));
            let into = tmp.join(format!("web-pack-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&into);
            std::fs::create_dir_all(&into).map_err(|err| err.to_string())?;
            let web = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../packages/web");
            pnpm(
                &web,
                &["pack", "--pack-destination", &into.display().to_string()],
                &tmp.join("smugglr-web-pack"),
            )?;
            std::fs::read_dir(&into)
                .map_err(|err| err.to_string())?
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .find(|path| path.extension().is_some_and(|ext| ext == "tgz"))
                .ok_or_else(|| "pnpm pack made no tarball".to_owned())
        })
        .clone()
}

/// Runs pnpm in `folder` with `args`, building Rust into `target`.
fn pnpm(folder: &Path, args: &[&str], target: &Path) -> Result<(), String> {
    let pnpm = if cfg!(windows) { "pnpm.cmd" } else { "pnpm" };
    let output = std::process::Command::new(pnpm)
        .current_dir(folder)
        .args(args)
        .env("CARGO_TARGET_DIR", target)
        .output()
        .map_err(|err| format!("failed to start pnpm: {err}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "pnpm {args:?} failed:\n{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

/// The app's page: it imports packages as a page with no build step does,
/// through an import map.
const APP_PAGE: &str = r#"<!doctype html>
<title>an app</title>
<script type="importmap">
{ "imports": { "@smugglr/web": "./node_modules/@smugglr/web/src/index.js" } }
</script>
"#;

/// Makes an app in `app` with @smugglr/web installed from the packed
/// package; the stand-in serves its folder.
pub(crate) fn install(app: &Path) -> Result<(), String> {
    let tarball = packed()?;
    std::fs::create_dir_all(app).map_err(|err| err.to_string())?;
    std::fs::write(
        app.join("package.json"),
        r#"{ "name": "an-app", "private": true, "type": "module" }"#,
    )
    .and_then(|()| std::fs::write(app.join("index.html"), APP_PAGE))
    .map_err(|err| err.to_string())?;
    pnpm(
        app,
        &["add", "--ignore-workspace", &tarball.display().to_string()],
        &app.join("target"),
    )
}

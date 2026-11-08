//! `smugglr --version` prints the version and exits 0: migratr checks it before it uses exec
//! (FR-SMUGGLR-084).

use std::process::Command;

#[test]
fn version_flag_prints_the_version() -> std::io::Result<()> {
    for flag in ["--version", "-V"] {
        let out = Command::new(env!("CARGO_BIN_EXE_smugglr"))
            .arg(flag)
            .output()?;
        assert!(out.status.success(), "{flag} exits 0");
        assert_eq!(
            String::from_utf8_lossy(&out.stdout).trim(),
            format!("smugglr {}", env!("CARGO_PKG_VERSION")),
            "{flag}"
        );
    }
    Ok(())
}

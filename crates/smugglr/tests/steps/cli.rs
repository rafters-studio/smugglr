//! Steps for smugglr sync on the command line.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use rstest_bdd_macros::{given, then, when};
use serde_json::Value;
use smugglr::{Options, sync};
use smugglr_place::LocalFile;

use crate::setup::{NOTES, ORDERS, THREE_NOTES, World, rows_of, sql_on};

#[given("a local file and a postgres:// address")]
fn file_and_postgres(world: &World) -> Result<(), String> {
    world.source.set(world.place("shop.db", NOTES)?);
    world
        .target
        .set("postgres://db.example.com/shop".to_owned());
    Ok(())
}

#[when("the agent runs smugglr sync on them")]
fn agent_syncs(world: &World) -> Result<(), String> {
    world.sync(&[]).map(|_| ())
}

#[then("smugglr refuses, saying smugglr keeps SQLite in step with SQLite")]
fn refuses_other_engine(world: &World) -> Result<(), String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    let said = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() && said.contains("smugglr keeps SQLite in step with SQLite") {
        Ok(())
    } else {
        Err(format!("smugglr exited {} saying {said:?}", output.status))
    }
}

#[given("two places where the source's orders table has rows the target lacks")]
fn source_orders_has_rows(world: &World) -> Result<(), String> {
    world.source.set(world.place(
        "source.db",
        &format!("{ORDERS}; INSERT INTO orders VALUES ('o1', 'tent'), ('o2', 'stove')"),
    )?);
    world.target.set(world.place("target.db", ORDERS)?);
    Ok(())
}

#[when("the agent runs smugglr sync on them without --json")]
fn sync_as_text(world: &World) -> Result<(), String> {
    world.sync(&[]).map(|_| ())
}

#[then("the output names orders with its status and counts in text")]
fn record_as_text(world: &World) -> Result<(), String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    let said = String::from_utf8_lossy(&output.stdout);
    let line = said
        .lines()
        .find(|line| line.trim_start().starts_with("orders"));
    match line {
        Some(line)
            if !said.trim_start().starts_with('{')
                && line.contains("moved")
                && line.contains("2 to target")
                && line.contains("0 to source") =>
        {
            Ok(())
        }
        _ => Err(format!("smugglr printed {said:?}")),
    }
}

const LINE_ITEMS: &str = "CREATE TABLE line_items (id TEXT PRIMARY KEY, qty INTEGER)";

/// A source whose notes and orders tables both have rows.
fn notes_and_orders_source() -> String {
    format!("{NOTES}; {THREE_NOTES}; {ORDERS}; INSERT INTO orders VALUES ('o1', 'tent')")
}

#[given("two places whose notes and orders tables both have rows to move")]
fn notes_and_orders(world: &World) -> Result<(), String> {
    world
        .source
        .set(world.place("source.db", &notes_and_orders_source())?);
    world
        .target
        .set(world.place("target.db", &format!("{NOTES}; {ORDERS}"))?);
    world.watched.set(format!("{:?}", orders_of(world)?));
    Ok(())
}

/// Both places' orders rows, as one list per place.
fn orders_of(world: &World) -> Result<[Vec<Vec<String>>; 2], String> {
    let source = world.source.get().ok_or("no source")?;
    let target = world.target.get().ok_or("no target")?;
    let query = "SELECT * FROM orders ORDER BY id";
    Ok([rows_of(&source, query)?, rows_of(&target, query)?])
}

#[when("the agent runs smugglr sync on them with --table notes")]
fn sync_notes_only(world: &World) -> Result<(), String> {
    world.sync(&["--table", "notes"]).map(|_| ())
}

#[then("no row of orders is written in either place")]
fn orders_untouched(world: &World) -> Result<(), String> {
    let before = world.watched.get().ok_or("no orders seen")?;
    let after = format!("{:?}", orders_of(world)?);
    let target = world.target.get().ok_or("no target")?;
    if rows_of(&target, "SELECT * FROM notes")?.is_empty() {
        return Err("notes did not move either".to_owned());
    }
    if before == after {
        Ok(())
    } else {
        Err(format!("orders went from {before} to {after}"))
    }
}

#[given("two places whose notes, orders and line_items tables all have rows to move")]
fn three_tables(world: &World) -> Result<(), String> {
    world.source.set(world.place(
        "source.db",
        &format!(
            "{}; {LINE_ITEMS}; INSERT INTO line_items VALUES ('l1', 2)",
            notes_and_orders_source()
        ),
    )?);
    world
        .target
        .set(world.place("target.db", &format!("{NOTES}; {ORDERS}; {LINE_ITEMS}"))?);
    Ok(())
}

#[when("the agent runs smugglr sync on them with --table notes --table orders")]
fn sync_two_tables(world: &World) -> Result<(), String> {
    world
        .sync(&["--table", "notes", "--table", "orders"])
        .map(|_| ())
}

#[then("the record lists notes and orders and no other table")]
fn lists_two_tables(world: &World) -> Result<(), String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    let said = String::from_utf8_lossy(&output.stdout);
    // The record as text names each table at the start of its line.
    let tables: Vec<&str> = said
        .lines()
        .filter(|line| line.starts_with("  ") && !line.starts_with("   "))
        .filter_map(|line| line.trim_start().split(':').next())
        .collect();
    if tables == ["notes", "orders"] {
        Ok(())
    } else {
        Err(format!("the record lists {tables:?}: {said}"))
    }
}

/// Makes one copy of the pair, syncs it once, then changes the same rows.
fn copy(world: &World, name: &str) -> Result<(String, String), String> {
    let source = world.place(&format!("{name}-source.db"), &notes_and_orders_source())?;
    let target = world.place(&format!("{name}-target.db"), &format!("{NOTES}; {ORDERS}"))?;
    let output = world.smugglr(&["sync", &source, &target])?;
    if !output.status.success() {
        return Err(format!("the first sync exited {}", output.status));
    }
    sql_on(
        &source,
        "INSERT INTO notes VALUES ('n4', 'fourth', 4); DELETE FROM notes WHERE id = 'n2'",
    )?;
    sql_on(&target, "UPDATE orders SET item = 'stove' WHERE id = 'o1'")?;
    Ok((source, target))
}

#[given("two copies of the same pair of places, with the same rows changed in each copy")]
fn two_copies(world: &World) -> Result<(), String> {
    world
        .copies
        .set(vec![copy(world, "one")?, copy(world, "two")?]);
    Ok(())
}

fn copies(world: &World) -> Result<[(String, String); 2], String> {
    let copies = world.copies.get().ok_or("no copies")?;
    <[(String, String); 2]>::try_from(copies).map_err(|_| "not two copies".to_owned())
}

fn printed(world: &World, args: &[&str]) -> Result<String, String> {
    let output = world.smugglr(args)?;
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

#[when("the agent syncs one copy with no --sync-type and the other with --sync-type direct")]
fn default_and_direct(world: &World) -> Result<(), String> {
    let [(a, b), (c, d)] = copies(world)?;
    world.records.set(vec![
        printed(world, &["sync", &a, &b])?,
        printed(world, &["sync", &c, &d, "--sync-type", "direct"])?,
    ]);
    Ok(())
}

#[when(
    "the agent syncs one copy with smugglr sync --json and a program syncs the other through the Rust crates"
)]
fn cli_and_crates(world: &World) -> Result<(), String> {
    let [(a, b), (c, d)] = copies(world)?;
    let cli = printed(world, &["sync", &a, &b, "--json"])?;
    let open = |path: &str| LocalFile::open(path).map_err(|err| err.to_string());
    let options = Options {
        config_dir: Some(world.config()?),
        ..Options::default()
    };
    let record = sync(&mut open(&c)?, &mut open(&d)?, &options).map_err(|err| err.to_string())?;
    world.records.set(vec![cli, record.json()]);
    Ok(())
}

#[then("the two records hold the same statuses, counts and keys")]
fn same_records(world: &World) -> Result<(), String> {
    let records = world.records.get().ok_or("no records")?;
    let [one, two] = <[String; 2]>::try_from(records).map_err(|_| "not two records")?;
    // As JSON when they are, else as the same text.
    let same = match (
        serde_json::from_str::<Value>(&one),
        serde_json::from_str::<Value>(&two),
    ) {
        (Ok(a), Ok(b)) => a == b,
        _ => one == two,
    };
    if same && one.contains("moved") {
        Ok(())
    } else {
        Err(format!("one record is {one}, the other {two}"))
    }
}

#[given("two places")]
fn two_places(world: &World) -> Result<(), String> {
    world
        .source
        .set(world.place("source.db", &format!("{NOTES}; {THREE_NOTES}"))?);
    world.target.set(world.place("target.db", NOTES)?);
    Ok(())
}

/// Runs `smugglr sync` with `flags` for `seconds`, then stops it, keeping
/// each line it printed with when it printed it.
fn watch_for(world: &World, flags: &[&str], seconds: u64) -> Result<(), String> {
    let source = world.source.get().ok_or("no source")?;
    let target = world.target.get().ok_or("no target")?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_smugglr"))
        .args(["sync", &source, &target])
        .args(flags)
        .env("XDG_CONFIG_HOME", world.xdg()?)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|err| format!("failed to start smugglr: {err}"))?;
    let stdout = child.stdout.take().ok_or("no stdout")?;
    let started = Instant::now();
    let reader = std::thread::spawn(move || {
        BufReader::new(stdout)
            .lines()
            .map_while(Result::ok)
            .map(|line| (started.elapsed().as_secs_f64(), line))
            .collect::<Vec<_>>()
    });
    std::thread::sleep(Duration::from_secs(seconds));
    child.kill().map_err(|err| err.to_string())?;
    let _ = child.wait();
    let lines = reader.join().map_err(|_| "the reader failed".to_owned())?;
    world.lines.set(lines);
    Ok(())
}

#[when(
    "the agent runs smugglr sync on them with --watch --json and stops it after several intervals"
)]
fn watch_json(world: &World) -> Result<(), String> {
    // Two intervals of 60 seconds and a little: runs at 0, 60 and 120.
    watch_for(world, &["--watch", "--json"], 130)
}

#[then("smugglr printed one record for each run it made")]
fn one_record_per_run(world: &World) -> Result<(), String> {
    let lines = world.lines.get().ok_or("smugglr was not watched")?;
    let text = lines
        .iter()
        .map(|(_, line)| line.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let records: Vec<Value> = serde_json::Deserializer::from_str(&text)
        .into_iter::<Value>()
        .collect::<Result<_, _>>()
        .map_err(|err| format!("smugglr printed what is not records: {err}: {text}"))?;
    let runs = 3;
    if records.len() == runs && records.iter().all(|record| record.get("status").is_some()) {
        Ok(())
    } else {
        Err(format!(
            "{runs} runs in 130 seconds, {} records: {text}",
            records.len()
        ))
    }
}

#[when("the agent runs smugglr sync on them without --watch")]
fn sync_without_watch(world: &World) -> Result<(), String> {
    world.sync(&[]).map(|_| ())
}

#[then("smugglr exits after printing one record")]
fn one_record(world: &World) -> Result<(), String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    let said = String::from_utf8_lossy(&output.stdout);
    let records = said
        .lines()
        .filter(|line| line.starts_with("smugglr sync:"))
        .count();
    if output.status.code().is_some() && records == 1 {
        Ok(())
    } else {
        Err(format!("smugglr exited {} printing {said}", output.status))
    }
}

#[when(
    "the agent runs smugglr sync on them with --watch and no number, and stops it after three minutes"
)]
fn watch_three_minutes(world: &World) -> Result<(), String> {
    watch_for(world, &["--watch"], 185)
}

#[then("smugglr made a run about every 60 seconds")]
fn every_sixty_seconds(world: &World) -> Result<(), String> {
    let lines = world.lines.get().ok_or("smugglr was not watched")?;
    let runs: Vec<f64> = lines
        .iter()
        .filter(|(_, line)| line.starts_with("smugglr sync:"))
        .map(|(at, _)| *at)
        .collect();
    let gaps: Vec<f64> = runs.windows(2).map(|pair| pair[1] - pair[0]).collect();
    if runs.len() == 4 && gaps.iter().all(|gap| (57.0..=63.0).contains(gap)) {
        Ok(())
    } else {
        Err(format!("runs at {runs:?} seconds"))
    }
}

#[given("the smugglr CLI")]
fn the_cli(world: &World) -> Result<(), String> {
    world.folder().map(|_| ())
}

#[when("the agent runs smugglr sync --help")]
fn sync_help(world: &World) -> Result<(), String> {
    world.smugglr(&["sync", "--help"]).map(|_| ())
}

#[then("the help lists no flag for ignoring tables")]
fn no_ignore_flag(world: &World) -> Result<(), String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    let help = String::from_utf8_lossy(&output.stdout);
    if !output.status.success() || !help.contains("usage: smugglr sync") {
        return Err(format!(
            "smugglr sync --help exited {} printing {help:?}",
            output.status
        ));
    }
    let flags: Vec<&str> = help
        .split(|c: char| c.is_whitespace() || "[]|".contains(c))
        .filter(|word| word.starts_with('-'))
        .collect();
    match flags
        .iter()
        .find(|flag| flag.to_ascii_lowercase().contains("ignore"))
    {
        Some(flag) => Err(format!("the help lists {flag}")),
        None => Ok(()),
    }
}

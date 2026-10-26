//! Rebuild the scenario runner when a `.feature` file is added under `tests/`.

fn main() {
    println!("cargo::rerun-if-changed=tests");
}

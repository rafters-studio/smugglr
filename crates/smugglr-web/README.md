# smugglr-web

The smugglr engine in the browser, compiled to wasm32 with wasm-bindgen, so a tab runs the same engine as the command line. `row_hash` is registered as the browser SQLite's `smugglr_row_hash` function, so each row is hashed where it lives. `tab` runs each sync, stash, retrieve and replay in the tab's worker; `rows` lets an app's server stash rows it hands to a tab whole.

Depend on it to build smugglr for a browser tab or for an app's server that feeds one.

smugglr's README: https://github.com/rafters-studio/smugglr#readme

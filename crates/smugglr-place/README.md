# smugglr-place

The places smugglr keeps in step. The `Place` trait is what every place speaks: it returns each row's key and hash, reads rows for the keys asked for, and writes the changes the engine decides. This crate provides a local SQLite file (`LocalFile`) and a service that speaks HTTP-SQL (`HttpSql`), with Cloudflare D1 reached through HTTP-SQL by its adapter (`D1`). `open` opens a place from its address.

The `native` feature, on by default, adds the local file and HTTP-SQL over HTTP. Without it, the `Place` trait and HTTP-SQL through a caller's own `Transport` build for wasm32.

Depend on it to write an adapter for a service smugglr does not know, or to open a place yourself.

smugglr's README: https://github.com/rafters-studio/smugglr#readme

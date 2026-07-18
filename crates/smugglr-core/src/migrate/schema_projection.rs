//! Pragma-derived semantic schema projection (pre-stub). Body lands in #290 --
//! a stable projection over `table_info` + `foreign_key_list` +
//! `index_list`/`index_info`, NOT a hash of `sqlite_master` text (which a
//! 12-step rebuild rewrites). Empty until then.

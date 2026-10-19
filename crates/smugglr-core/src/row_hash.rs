//! The row hash: one definition, the same native and in WASM (FR-SMUGGLR-058).
//!
//! A row is hashed from SQLite's `quote()` text of each of its columns, in
//! column order, so every runtime that reads a row through SQLite sees the
//! same bytes (toy D) and a place can run the same hash as a SQL function.
//! `quote()` already writes NULL, empty text and an empty blob apart (`NULL`,
//! `''`, `X''`); each column's text is prefixed with its byte length, so no
//! column's text can run into the next. The hash is BLAKE3 over that input,
//! kept to its first 16 bytes.

/// Hashes one row, given SQLite's `quote()` text of each column in order.
#[must_use]
pub fn row_hash(columns: &[&str]) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    for column in columns {
        hasher.update(&(column.len() as u64).to_be_bytes());
        hasher.update(column.as_bytes());
    }
    let mut hash = [0; 16];
    hash.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    hash
}

#[cfg(test)]
mod tests {
    use super::row_hash;

    #[test]
    fn a_column_boundary_changes_the_hash() {
        assert_ne!(row_hash(&["'ab'", "'c'"]), row_hash(&["'a'", "'bc'"]));
    }
}

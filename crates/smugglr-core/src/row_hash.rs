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
    let mut hasher = RowHasher::new();
    for column in columns {
        hasher.length(column.len() as u64);
        hasher.piece(column.as_bytes());
    }
    hasher.finish()
}

/// The row hash fed one column at a time, and a column's `quote()` text in
/// pieces, so a value too large to hold is hashed as it is read: the same
/// hash [`row_hash`] gives.
#[derive(Debug, Clone, Default)]
pub struct RowHasher(blake3::Hasher);

impl RowHasher {
    /// A row hash with no column yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts the next column, whose `quote()` text is `len` bytes.
    pub fn length(&mut self, len: u64) {
        self.0.update(&len.to_be_bytes());
    }

    /// The next piece of the column's `quote()` text.
    pub fn piece(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    /// The row's hash.
    #[must_use]
    pub fn finish(&self) -> [u8; 16] {
        let mut hash = [0; 16];
        hash.copy_from_slice(&self.0.finalize().as_bytes()[..16]);
        hash
    }
}

#[cfg(test)]
mod tests {
    use super::{RowHasher, row_hash};

    #[test]
    fn a_column_boundary_changes_the_hash() {
        assert_ne!(row_hash(&["'ab'", "'c'"]), row_hash(&["'a'", "'bc'"]));
    }

    #[test]
    fn a_column_fed_in_pieces_hashes_as_a_whole() {
        let mut hasher = RowHasher::new();
        hasher.length(3);
        hasher.piece(b"'a");
        hasher.piece(b"'");
        hasher.length(4);
        hasher.piece(b"X'00'");
        assert_ne!(hasher.finish(), row_hash(&["'a'", "X'00'"]));
        let mut hasher = RowHasher::new();
        hasher.length(3);
        hasher.piece(b"'a");
        hasher.piece(b"'");
        hasher.length(5);
        hasher.piece(b"X'00'");
        assert_eq!(hasher.finish(), row_hash(&["'a'", "X'00'"]));
    }
}

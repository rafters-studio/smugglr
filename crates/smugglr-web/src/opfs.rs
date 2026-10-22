//! The tab's memory (FR-SMUGGLR-072): smugglr's own SQLite database in the
//! tab's OPFS storage, beside the app's database and never a table in it.
//! It holds each key's last hash for the pair (the tab, the endpoint), the
//! journal, the graveyard and the dropped log, one row per memory file.
//! Clearing the site's data clears it with the app's database.

use smugglr_memory::{Error, Store};

use crate::host;

/// The tab's memory store.
#[derive(Debug)]
pub(crate) struct Opfs;

impl Store for Opfs {
    fn read(&self, path: &str) -> Result<Option<String>, Error> {
        host::mem_read(path).map_err(Error::Store)
    }

    fn replace(&self, path: &str, text: &str) -> Result<(), Error> {
        host::mem_replace(path, text).map_err(Error::Store)
    }

    fn append(&self, path: &str, text: &str) -> Result<(), Error> {
        host::mem_append(path, text).map_err(Error::Store)
    }

    fn remove(&self, path: &str) -> Result<(), Error> {
        host::mem_remove(path).map_err(Error::Store)
    }
}

// A table of the tab's database as a bridge reads and writes it
// (FR-SMUGGLR-082): each row is one item, an object of its columns, under
// the text of its primary key. The same file is in @smugglr/zustand.

const ident = (name) => `"${name.replaceAll('"', '""')}"`;

// A key as table-changed names it, its column's quote() text, as the store
// keys it.
const unquote = (text) => (text.startsWith("'") ? text.slice(1, -1).replaceAll("''", "'") : text);

/** Binds table `name` of `tab`, a Tab from @smugglr/web's open(). */
export async function table(tab, name) {
  const info = await tab.query('SELECT name, pk FROM pragma_table_info(?) ORDER BY cid', [name]);
  const keyed = info.filter(([, pk]) => pk > 0);
  if (keyed.length !== 1) {
    throw new Error(`${name}: a bound table needs a primary key of one column`);
  }
  const columns = info.map(([column]) => column);
  const [[key]] = keyed;
  const select = `SELECT ${columns.map(ident).join(', ')} FROM ${ident(name)}`;
  const items = (rows) =>
    Object.fromEntries(
      rows.map((row) => {
        const item = Object.fromEntries(columns.map((column, i) => [column, row[i]]));
        return [String(item[key]), item];
      }),
    );
  return {
    /** Every row, by key. */
    all: async () => items(await tab.query(select)),
    /** The rows of `keys`, as table-changed names them, by key; a row gone is undefined. */
    async some(keys) {
      const texts = keys.map(([text]) => text);
      const found = items(
        await tab.query(`${select} WHERE quote(${ident(key)}) IN (SELECT value FROM json_each(?))`, [
          JSON.stringify(texts),
        ]),
      );
      return texts.map((text) => [unquote(text), found[unquote(text)]]);
    },
    /**
     * Writes `item` as the row of `id`, or deletes the row when `item` is
     * undefined. When the tab refuses, calls `putBack(id, row)` with what
     * the table holds for `id` (undefined when nothing), then `refused(err)`;
     * when reading what the table holds fails too, `refused` hears that as well.
     */
    async save(id, item, putBack, refused) {
      try {
        await (item === undefined
          ? tab.exec(`DELETE FROM ${ident(name)} WHERE ${ident(key)} = ?`, [id])
          : tab.exec(
              `INSERT OR REPLACE INTO ${ident(name)} (${columns.map(ident).join(', ')}) VALUES (${columns.map(() => '?').join(', ')})`,
              columns.map((column) => (column === key ? id : (item[column] ?? null))),
            ));
      } catch (err) {
        let unread;
        try {
          putBack(id, items(await tab.query(`${select} WHERE ${ident(key)} = ?`, [id]))[id]);
        } catch (reading) {
          unread = reading;
        }
        refused(err);
        if (unread !== undefined) {
          refused(unread);
        }
      }
    },
    /** Calls `heard` with the keys of each table-changed naming this table; returns its stop. */
    listen(heard) {
      const changed = ({ detail }) => {
        if (detail.table === name) {
          heard(detail.keys);
        }
      };
      tab.addEventListener('table-changed', changed);
      return () => tab.removeEventListener('table-changed', changed);
    },
  };
}

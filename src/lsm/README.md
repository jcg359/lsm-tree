# LSM Tree

This package wires the raw data structures from [`src/dsa`](../dsa/README.md) into the three main LSM tree operations: writes, reads, and compaction.

---

## `memtable.py` - `LSMTreeMemtable`

Manages the active in-memory write buffer. Wraps a `SkipList` and handles:

- **`insert(customer_id, raw) -> (key, value) | None`** - Parses a `room-device,temperature,humidity` string, builds a `customer#room-device` key, and inserts into the skip list. Returns `(key, value_dict)` on success so callers can forward the record to the WAL. Returns `None` on validation failure (temperature must include a scale suffix `F` or `C`; humidity must be 1–100).
- **`flush_if_full()`** - When the memtable reaches `max_memtable_count` entries, flushes it to a new L0 SSTable file and resets the active memtable. Returns the new file ID, or `None` if no flush occurred.
- **`init_memtable()`** - Resets the active memtable to a fresh empty skip list.

---

## `search.py` - `LSMTreeSearch`

Coordinates key lookup across the memtable and all SSTable levels. Search order: memtable first, then L0, then L1+. Returns `(value, source)`.

- **`search(key)`** - Full lookup across all layers. When a tombstone is found at any layer the search stops immediately (no lower levels are consulted) and returns `(None, source)` where `source` has a `-x` suffix to indicate a tombstone hit (e.g. `"MT-x"`, `"L0-x"`). A live value returns `(value, source)` with a plain source label. If the key is absent everywhere returns `(None, "L{max_level}")`.
- **`update_memtable(memtable)`** - Swaps in a new memtable reference after a flush.
- **`update_last_id(level, last_id)`** - Updates the newest known file ID at a given level after a flush or compaction.

---

## `wal.py` - `WriteAheadLog`

Durability log that mirrors every memtable write to `L0/wal.jsonl` before the memtable is flushed to an SSTable. Each line is a JSON record `{"key": …, "value": …}` - the value is the live data dict for inserts or the tombstone sentinel for deletes.

- **`append(key, value)`** - Append one record to the WAL.
- **`delete()`** - Remove `wal.jsonl` from disk. Called after a successful L0 flush (the data is now in an SSTable) and after a full truncate.
- **`path`** _(property)_ - Absolute path to the WAL file.

On startup the controller reads the WAL (if present) and replays all records into the fresh memtable before accepting new commands, recovering any writes that were not yet flushed in the previous session.

---

## `compact.py` - `LSMTreeCompator`

Drives compaction from L0 into L1. On each call:

1. Picks the oldest L0 SSTable.
2. Finds overlapping L1 files by key range.
3. Merges all of them into new L1 SSTables (duplicate keys resolved, tombstones honored).
4. Deletes the compacted L0 file and any L1 files that were replaced.

- **`compact_level_zero(last_l1_id)`** - Run one round of L0→L1 compaction. Returns the new newest L1 file ID.
- **`newest_file_id(level)`** - Returns the highest ULID at the given level.

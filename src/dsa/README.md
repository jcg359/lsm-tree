# DSA - Data Structures & Algorithms

---

## `memtable/`

### `skip_list.py`

#### `SkipListNode`

Internal node for the skip list. Holds a key, value, and a `forward` list of next-pointers - one slot per level - enabling O(log n) traversal by skipping over nodes at higher levels.

#### `SkipList`

A sorted in-memory key-value store (memtable). Keys are kept in sorted order at all times. Deletes are soft - recording a tombstone value excluded from iteration; tombstones are written through to SSTables on flush so compaction can resolve them.

**Constructor:**

| Parameter | Default | Effect |
|-----------|---------|--------|
| `max_level` | `3` | Maximum number of express lanes in the skip list. Higher values reduce average search time at the cost of more memory per node. |
| `block_size` | `10` | Records per block when flushing to an SSTable. |

**Methods**

| Method | Description |
|--------|-------------|
| `insert(key, value)` | Insert or overwrite. Revives a tombstoned key. |
| `search(key)` | Return value or `None` if absent or deleted. |
| `delete(key) -> bool` | Soft-delete via tombstone. Returns `False` if key was not live. |
| `count() -> int` | Number of live (non-tombstoned) entries. |
| `ordered_keys()` | Iterator over keys in sorted order, tombstones excluded. |
| `flush_to_level_zero(root_folder) -> (data_path, file_id)` | Write all entries (including tombstones) to a new SSTable pair under `root_folder/L0/`. Returns `(data_path, file_id)`. |

**Time complexity**

| Operation | Average | Worst case |
|-----------|---------|------------|
| `insert` | O(log n) | O(n) |
| `search` | O(log n) | O(n) |
| `delete` | O(log n) | O(n) |
| `iterate` | O(n) | O(n) |

Average case holds because each node's level is determined by independent coin flips at insert time - the probability that any node is promoted to level k falls off as (½)^k. Worst case requires every coin flip to produce the maximum level for every node, which is astronomically unlikely in practice.

---

## `sst/`

Sorted String Table (SSTable) file format and operations. All files in this package share a common on-disk layout:

```
<root>/
  L0/   <ULID>.jsonl              one JSON record per line: {"key": …, "value": …}
        <ULID>.index.jsonl        block index: {"block", "first_key", "offset", "record_count"}
  L1/   …
```

Files within L0 may have overlapping key ranges; files within L1+ have non-overlapping ranges.

---

### `utility.py`

Shared constants, path helpers, and configuration classes used across the `sst` package.

**Free functions**

| Function | Returns |
|----------|---------|
| `level_dir(root_path, level)` | `root_path/L{level}` |
| `data_path(folder, file_id)` | `folder/{file_id}.jsonl` |
| `index_path(folder, file_id)` | `folder/{file_id}.index.jsonl` |
| `tombstone()` | The sentinel string used to mark deleted keys. |
| `ulid_max()` | Largest valid ULID string (used as an upper bound). |
| `ulid_min()` | Smallest valid ULID string (used as a lower bound). |

#### `SortedLevelConfiguration`

Per-level tuning parameters.

| Parameter | Default | Effect |
|-----------|---------|--------|
| `block_size` | `10` | Records per block written to output SSTables at this level. |
| `blocks_per_file` | `20` | Maximum blocks before the writer starts a new output file. |
| `min_files` | `2` | Target minimum number of output files after compaction. Drives split-key planning. |

#### `SortedTableConfiguration`

Maps level numbers to `SortedLevelConfiguration` instances. Pass to `SortedTableCompactor`.

```python
config = SortedTableConfiguration({
    1: SortedLevelConfiguration(block_size=10, blocks_per_file=20, min_files=4),
})
```

| Method | Description |
|--------|-------------|
| `for_level(level) -> SortedLevelConfiguration` | Returns the configuration for the given level. |

---

### `read.py`

Low-level file I/O for SSTable files. Shared by `search.py` and `compact.py` - neither duplicates file access logic.

#### `SortedTableCursor`

Dataclass tracking read position within a single SSTable file during a k-way merge. Holds the current in-memory block, position within that block, the full block index, and a priority integer used to break ties (lower priority = higher precedence, so L0 beats L1).

| Field | Description |
|-------|-------------|
| `records` | Records in the currently loaded block. |
| `pos` | Index of the current record within `records`. |
| `priority` | Tie-breaking rank; lower wins (0 = L0, 1 = L1, …). |
| `folder` | Directory containing this file. |
| `file_id` | ULID of the file being read. |
| `blocks` | Full block index for the file. |
| `block_idx` | Index of the currently loaded block within `blocks`. |
| `current` _(property)_ | The record at the current position. |

#### `SortedTableReader`

**Constructor:** `SortedTableReader(root_data_path)`

| Method | Description |
|--------|-------------|
| `list_file_ids(folder, last_id) -> List[str]` | ULIDs of all data files in `folder` with id ≤ `last_id`. |
| `read_index(folder, file_id) -> List[dict]` | Load full block index into memory. |
| `read_block(folder, file_id, block) -> List[dict]` | Seek to a block's byte offset and read its records. |
| `get_key_range(folder, file_id) -> (min_key, max_key)` | Key range for a file; reads index + final block only. Returns `None` if the file is empty. |
| `get_level_counts(last_ids, max_level) -> List[dict]` | For each level 0–`max_level`, count all live records across files with id ≤ `last_ids[level]`. Returns a list of `{"sst_level", "key_count"}` dicts. |
| `make_cursor(folder, file_id, priority) -> SortedTableCursor` | Open a file as a cursor positioned at the first record. Returns `None` if the file is empty. |
| `advance_cursor(cursor) -> bool` | Move to the next record, loading the next block from disk when the current one is exhausted. Returns `False` when the file is fully consumed. |

---

### `write.py`

Writes sorted records to new ULID-named SSTable pairs (data file + block index).

#### `SortedTableWriter`

**Constructor:** `SortedTableWriter(root_data_path)`

| Method | Description |
|--------|-------------|
| `write(level, block_size, records) -> (data_path, file_id)` | Write a new SSTable at the given level. Write a new block index entry every `block_size` records. Returns the file's data path and ULID. |
| `write_split(level, records, split_keys, block_size, max_blocks_per_file) -> List[str]` | `write` partitioned across multiple files: based on `split_keys`, or when `max_blocks_per_file` blocks have been written. Returns the list of new file IDs. |
| `preserve_files(level, file_ids) -> str` | Remove all files at `level` that are not in `file_ids`. Returns the newest (highest ULID) surviving file ID at that level. |
| `remove_file(level, file_id)` | Delete the data and index files for the given ULID. |

---

### `search.py`

Key lookup over an SSTable directory tree.

#### `SortedTableSearch`

Delegates all file I/O to a `SortedTableReader`.

**Constructor:** `SortedTableSearch(reader: SortedTableReader)`

**`search(key, level, last_id="") -> value | None`** dispatches to one of two strategies:

**Level 0** - files may have overlapping key ranges as memtables flush before compaction. Files are scanned in descending ULID order (newest first). The first file that contains the key - including a tombstone - is authoritative; older files are not consulted.

**Level 1+** - files have non-overlapping key ranges. A binary search over each file's `first_key` (read from the block index) identifies the single candidate file in O(log F) where F is the number of files. Within the candidate file a second binary search over the block index locates the right block in O(log B) where B is the number of blocks. Only that one block is read from disk.

---

### `compact.py`

Merges L0 SSTables into L1, resolving duplicate keys and tombstones.

#### `SortedTableCompactor`

Uses a merge-sort style k-way merge: one `SortedTableCursor` per input file is opened, and each step selects the cursor with the globally smallest current key (ties broken by priority - L0 = 0 beats L1 = 1). This produces a single sorted stream from arbitrarily many sorted input files without loading more than one block per file into memory at a time. Duplicate keys are resolved by discarding any record whose key matches the most recently yielded key (the higher-priority source always appears first).

**Constructor:** `SortedTableCompactor(root_data_path, config: SortedTableConfiguration)`

| Method | Description |
|--------|-------------|
| `compact_level_zero(last_l1_id) -> (compacted_l0_id, surviving_l1_ids)` | Picks the oldest L0 file, finds which L1 files overlap its key range (index reads only), merges them, and writes new L1 SSTables. Returns the compacted L0 file ID (or `None` if L0 was empty) and the list of surviving L1 file IDs. |
| `newest_file_id(level) -> str \| None` | Returns the highest ULID at the given level, or `None` if the level is empty. |

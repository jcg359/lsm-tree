# lsm-tree

An LSM (Log-Structured Merge) tree implementation in Python, built from scratch with an interactive demo.

---

## Running the demo

From the project root:

```bash
bash run-demo.sh
```

This installs dependencies via Poetry and launches an interactive prompt. Type `help` at the prompt to see all available commands.

---

## Project structure
### [`src/demo`](src/demo/README.md) — Interactive demo

A command-line REPL that simulates an IoT sensor storage system. Customers submit temperature and humidity readings keyed by `customer#room-device`. Demonstrates inserts, searches, deletes, flushes, and compaction against real on-disk SSTable files.

### [`src/dsa`](src/dsa/README.md) — Data structures

The foundational building blocks: a skip list (memtable) and a full SSTable layer (read, write, search, compact). No LSM-specific logic — these are general-purpose sorted data structures.

### [`src/lsm`](src/lsm/README.md) — LSM tree

Wires the DSA layer into the three core LSM operations:

- **Memtable** — buffered in-memory writes with automatic L0 flush when full
- **Search** — key lookup across the memtable and all SSTable levels
- **Compaction** — merges L0 SSTables into L1, resolving duplicates and tombstones



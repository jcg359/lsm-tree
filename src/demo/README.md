# Demo

An interactive command-line demo of the LSM tree. Simulates an IoT sensor storage system where customers submit temperature and humidity readings from named room/device sensors.

Data is persisted to `src/data/` as JSONL SSTable files.

---

## Running the demo

**Prerequisites:** Python 3.13+ and [pipx](https://pipx.pypa.io/) must be installed. The scripts install Poetry and all project dependencies automatically.

**Linux / macOS:**

```bash
bash ./scripts/poetry-run-demo.sh
```

**Windows:**

```bat
scripts\poetry-run-demo.bat
```

> Also installs `pyreadline3` for readline support and handles pipx PATH setup automatically.

This installs dependencies via Poetry and launches the interactive prompt.

---

## Commands

| Command | Arguments | Description |
|---------|-----------|-------------|
| `load` | `[count] [customers]` | Insert demo sensor entries. Defaults to filling the memtable with 1 random customer. |
| `search` | `[key]` | Look up a key (format: `customer#room-device`). Prompts if not provided. |
| `delete` | `[key]` | Soft-delete a key via tombstone. Prompts if not provided. |
| `input` | | Interactively enter a customer ID and sensor reading (`room-device,temp,humidity`). |
| `compact` | | Merge the oldest L0 SSTable into L1. |
| `truncate` | | Delete all data files, reset the memtable, and delete the WAL (prompts for confirmation). |
| `count` | | Show live record counts in the memtable and each SSTable level. |
| `memtable` | | List all keys currently held in the memtable. |
| `help` | | Print a summary of all commands. |
| `exit` | | Exit the demo. |

---

## Files

| File | Description |
|------|-------------|
| `main.py` | REPL entry point - replays the WAL on startup, then runs the command loop. |
| `controller.py` | Coordination layer between the REPL and the LSM tree modules. Write ahead log maintained and used to restore memtable on startup via `restore_memtable_wal()`. |
| `utility.py` | Random data generation (customers, sensor readings) and file helpers. |

# Demo

An interactive command-line demo of the LSM tree. Simulates an IoT sensor storage system where customers submit temperature and humidity readings from named room/device sensors.

Data is persisted to `src/data/` as JSONL SSTable files.

---

## Running the demo

From the project root:

```bash
bash run-demo.sh
```

This installs dependencies via Poetry and launches the interactive prompt.

---

## Commands

| Command | Arguments | Description |
|---------|-----------|-------------|
| `load` | `[count] [customers]` | Insert demo sensor entries. Defaults to filling the memtable with 1 random customer. |
| `search` | `[key]` | Look up a key (format: `customer#room-device`). Prompts if not provided. |
| `delete` | `[key]` | Soft-delete a key from the memtable via tombstone. Prompts if not provided. |
| `input` | | Interactively enter a customer ID and sensor reading (`room-device,temp,humidity`). |
| `compact` | | Merge the oldest L0 SSTable into L1. |
| `truncate` | | Delete all data files and reset the memtable (prompts for confirmation). |
| `count` | | Show live record counts in the memtable and each SSTable level. |
| `memtable` | | List all keys currently held in the memtable. |
| `help` | | Print a summary of all commands. |
| `exit` | | Exit the demo. |

---

## Files

| File | Description |
|------|-------------|
| `main.py` | REPL loop — parses input and dispatches to controller functions. |
| `controller.py` | Thin coordination layer between the REPL and the LSM tree modules. |
| `utility.py` | Random data generation (customers, sensor readings) and file helpers. |

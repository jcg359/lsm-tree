import json
import os
from typing import List

import src.lsm.memtable as lsm_t
import src.lsm.search as lsm_s
import src.lsm.compact as lsm_c
import src.lsm.wal as lsm_w

import src.dsa.sst.utility as sst_u
import src.demo.utility as util


data_path = util.data_root_path()

mt = lsm_t.LSMTreeMemtable(max_memtable_count=100, data_root_path=data_path)
compactor = lsm_c.LSMTreeCompator(data_root_path=data_path)
wal = lsm_w.WriteAheadLog(data_path)

last_ids = {1: compactor.newest_file_id(1)}
sst = lsm_s.LSMTreeSearch(memtable=mt.get_current(), data_root_path=data_path, max_sst_levels=1, last_file_ids=last_ids)


def save(customer_id: str, input: str):
    # if insert causes a L0 flush
    flushed_id = mt.flush_if_full()
    if flushed_id is not None:
        # supply new memtable for writes
        sst.update_memtable(mt.get_current())
        # and consider this new L0 id
        sst.update_last_id(0, flushed_id)
        # WAL has been persisted to L0; reset it
        wal.delete()

    # save the new item
    result = mt.insert(customer_id, input)
    if result is not None:
        wal.append(*result)


def level_counts(memtable_only: bool = False):
    print(f"count in memtable = {mt.get_current().count()}")
    if memtable_only:
        return
    for level in sst.level_counts():
        print(level)


def memtable_keys():
    print("Keys in Memtable:")
    keys = " ".join([f"< {i} >" for i in mt.get_current().ordered_keys()])
    print(keys if len(keys) > 0 else "None")


def truncate():
    if input(f"confirm clear data from {data_path} (Y or N):").strip().upper() == "Y":
        util.delete_data_files(data_path)
        mt.init_memtable()
        sst.update_memtable(mt.get_current())
        level_counts()
        wal.delete()


def compact():
    last_id = compactor.compact_level_zero(sst.get_last_id(1))
    sst.update_last_id(1, last_id)
    level_counts()


def save_input():
    customer_id = input("enter customer-id: ")
    customer_id = (customer_id or "").strip().lower()

    raw = input("enter room-device,temperature,humidity (csv): ")
    save(customer_id, raw)
    level_counts(True)


def load(parts: List[str]):
    raw = parts[1] if len(parts) > 1 else input("enter count to insert (default=fill memtable): ")
    target = util.try_to_int(raw) or (mt._max_memtable_count - mt.get_current().count())

    raw = parts[2] if len(parts) > 2 else input("enter customer count (default=use current): ")
    customers = util.random_customers(raw)

    print(f"creating {target} demo entries")
    print(f"for customers {customers}")

    while target > 0:
        cust = (target - 1) % len(customers)
        save(customers[cust], util.random_sensor_data())
        target -= 1

    level_counts(True)


def _parse_or_input_key(parts: List[str]):
    raw = parts[1] if len(parts) > 1 else input("enter # separated key: ")
    return raw


def search(parts: List[str]):
    result, source = sst.search(_parse_or_input_key(parts).strip())

    ts_source = f" ({source})" if source.find(sst_u.tombstone_source()) >= 0 else ""
    print(f"{result} ({source})" if result is not None else f"__not_found__{ts_source}")


def delete(parts: List[str]):
    key, value = mt.get_current().delete(_parse_or_input_key(parts))
    wal.append(key, value)
    print(f"deleted {key}")


def restore_memtable_wal():
    if not os.path.exists(wal.path):
        return
    restored = 0
    with open(wal.path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            key, value = record["key"], record["value"]
            if value == sst_u.tombstone():
                mt.get_current().delete(key)
            else:
                mt.get_current().insert(key, value)
            restored += 1
    print(f"restored {restored} memtable keys from WAL")


def help():
    print("Available commands:")
    print("  load [count] [customers]  - Bulk demo load. Prompts for number of entries and customers if not provided.")
    print("  search [key]              - Search for a key (format: customer#room-device). Prompts if not provided.")
    print("  delete [key]              - Delete a key from the memtable. Prompts if not provided.")
    print("  input                     - Manually enter a customer-id and sensor data (room-device,temp,humidity).")
    print("  truncate                  - Clear all data files and reset the memtable (prompts for confirmation).")
    print("  compact                   - Compact L0 SST files into L1.")
    print("  count                     - Show the number of entries in the memtable and each SST level.")
    print("  memtable                  - List all keys currently in the memtable.")
    print("  exit                      - Exit the demo.")

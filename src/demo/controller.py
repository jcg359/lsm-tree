import json
import os
from typing import List

import src.lsm.memtable as lsm_t
import src.lsm.search as lsm_s
import src.lsm.compact as lsm_c
import src.lsm.wal as lsm_w

import src.dsa.sst.utility as sst_u
import src.demo.utility as util


class LSMController:
    def __init__(self, data_path: str = None):
        self._data_path = data_path or util.data_root_path()

        self._mt = lsm_t.LSMTreeMemtable(max_memtable_count=100, data_root_path=self._data_path)
        self._compactor = lsm_c.LSMTreeCompator(data_root_path=self._data_path)
        self._wal = lsm_w.WriteAheadLog(self._data_path)

        last_ids = {1: self._compactor.newest_file_id(1)}
        self._sst = lsm_s.LSMTreeSearch(
            memtable=self._mt.get_current(),
            data_root_path=self._data_path,
            max_sst_levels=1,
            last_file_ids=last_ids,
        )

    def save(self, customer_id: str, input: str):
        # if insert causes a L0 flush
        flushed_id = self._mt.flush_if_full()
        if flushed_id is not None:
            # supply new memtable for writes
            self._sst.update_memtable(self._mt.get_current())
            # and consider this new L0 id
            self._sst.update_last_id(0, flushed_id)
            # WAL has been persisted to L0; reset it
            self._wal.delete()

        # save the new item
        result = self._mt.insert(customer_id, input)
        if result is not None:
            self._wal.append(*result)

    def level_counts(self, memtable_only: bool = False):
        results = [{"lsm_level": "MT", "key_count": self._mt.get_current().count()}]
        if not memtable_only:
            results.extend(self._sst.level_counts())
        for r in results:
            print(f"{r['lsm_level']} keys = {r['key_count']}")
        return results

    def memtable_keys(self):
        print("Keys in Memtable:")
        key_list = list(self._mt.get_current().ordered_keys())
        formatted = " ".join([f"< {i} >" for i in key_list])
        print(formatted if len(formatted) > 0 else "None")
        return key_list

    def truncate_input(self):
        self.truncate(input(f"confirm clear data from {self._data_path} (Y or N):"))

    def truncate(self, confirm: str):
        if confirm.strip().upper() != "Y":
            print("exiting truncate")
            return

        util.delete_data_files(self._data_path)
        self._mt.init_memtable()
        self._sst.update_memtable(self._mt.get_current())

        self.level_counts()
        self._wal.delete()

    def compact(self):
        last_id = self._compactor.compact_level_zero(self._sst.get_last_id(1))
        self._sst.update_last_id(1, last_id)
        self.level_counts()

    def save_input(self):
        customer_id = input("enter customer-id: ")
        customer_id = (customer_id or "").strip().lower()

        raw = input("enter room-device,temperature,humidity (csv): ")
        self.save(customer_id, raw)
        self.level_counts(True)

    def load_input(self, parts: List[str]):
        raw = parts[1] if len(parts) > 1 else input("enter count to insert (default=fill memtable): ")
        target = util.try_to_int(raw) or (self._mt._max_memtable_count - self._mt.get_current().count())

        raw = parts[2] if len(parts) > 2 else input("enter customer count (default=use current): ")
        self.load(raw, target)

        self.level_counts(True)

    def load(self, raw, target):
        customers = util.random_customers(raw)

        print(f"creating {target} demo entries")
        print(f"for customers {customers}")

        while target > 0:
            cust = (target - 1) % len(customers)
            self.save(customers[cust], util.random_sensor_data())
            target -= 1

    def _parse_or_input_key(self, parts: List[str]):
        raw = parts[1] if len(parts) > 1 else input("enter # separated key: ")
        return raw

    def search_input(self, parts: List[str]):
        key = self._parse_or_input_key(parts).strip()
        return self.search(key)

    def search(self, key):
        result, source = self._sst.search(key)

        ts_source = f" ({source})" if source.find(sst_u.tombstone_source()) >= 0 else ""
        print(f"{result} ({source})" if result is not None else f"__not_found__{ts_source}")
        return result, source

    def delete(self, parts: List[str]):
        key, value = self._mt.get_current().delete(self._parse_or_input_key(parts))
        self._wal.append(key, value)
        print(f"deleted {key}")

    def restore_memtable_wal(self):
        if not os.path.exists(self._wal.path):
            return

        restored = 0
        mt = self._mt.get_current()
        with open(self._wal.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                key, value = record["key"], record["value"]
                if value == sst_u.tombstone():
                    mt.delete(key)
                else:
                    mt.insert(key, value)
                restored += 1
        print(f"restored {mt.count()} memtable keys from WAL")

    def help(self):
        print("Available commands:")
        print(
            "  load [count] [customers]  - Bulk demo load. Prompts for number of entries and customers if not provided."
        )
        print("  search [key]              - Search for a key (format: customer#room-device). Prompts if not provided.")
        print("  delete [key]              - Delete a key from the memtable. Prompts if not provided.")
        print("  input                     - Manually enter a customer-id and sensor data (room-device,temp,humidity).")
        print("  truncate                  - Clear all data files and reset the memtable (prompts for confirmation).")
        print("  compact                   - Compact L0 SST files into L1.")
        print("  count                     - Show the number of entries in the memtable and each SST level.")
        print("  memtable                  - List all keys currently in the memtable.")
        print("  exit                      - Exit the demo.")

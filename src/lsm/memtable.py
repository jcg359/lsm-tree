import re
import os

from src.dsa.memtable.skip_list import SkipList
import src.dsa.sst.write as sst_write


class LSMTreeMemtable:
    _max_memtable_count = 100
    _data_root_path = ""

    def __init__(
        self,
        max_memtable_count: int,
        data_root_path: str,
        memtable_skip_levels: int = 3,
        index_block_size: int = 10,
    ):
        self._max_memtable_count = max_memtable_count

        self._data_root_path = data_root_path
        os.makedirs(self._data_root_path, exist_ok=True)
        self._current = SkipList(block_size=index_block_size, max_level=memtable_skip_levels)

    def set_max_memtable_count(self, value: int):
        self._max_memtable_count = value

    def get_current(self):
        return self._current

    def sanitize_key(self, s: str) -> str:
        s = s.lower()
        s = re.sub(r"[^a-z0-9-]", "-", s)
        return s

    def make_key(self, customer_id: str, room_device: str) -> str:
        if customer_id is None:
            print("error: customer id must be set")
            return

        return f"{self.sanitize_key(customer_id)}#{self.sanitize_key(room_device)}"

    def memtable_keys(self):
        return self._current.ordered_keys()

    def flush_if_full(self):
        if self._current.count() >= self._max_memtable_count:
            flush = self._current
            self.init_memtable()

            write_records = sst_write.SortedTableWriter(self._data_root_path).write
            _, file_id = flush.flush_to_level_zero(write_records)
            print(f"created L0 file id: {file_id}")
            return file_id

        return None

    def init_memtable(self):
        self._current = SkipList(block_size=self._current.block_size, max_level=self._current.max_level)

    def insert(self, customer_id: str, raw: str) -> tuple | None:
        parts = [p.strip() for p in raw.split(",")]

        if len(parts) != 3:
            print("error: expected 3 comma-separated values")
            return None

        room_device, temperature_raw, humidity_raw = parts

        key = self.make_key(customer_id, room_device)

        temp_match = re.fullmatch(r"(-?\d+(?:\.\d+)?)([fFcC])", temperature_raw)
        if not temp_match:
            print("error: temperature must be a number followed by F or C (e.g. 72.5F or 22c)")
            return None

        temp_number = temp_match.group(1)
        scale = temp_match.group(2).upper()

        try:
            humidity = float(humidity_raw)
            if not (1 <= humidity <= 100):
                raise ValueError
        except ValueError:
            print("error: humidity must be a number between 1 and 100")
            return None

        value = {
            "temperature": temp_number,
            "scale": scale,
            "humidity": humidity_raw,
        }
        self._current.insert(key, value)
        print(f"inserted: {key}")
        return key, value

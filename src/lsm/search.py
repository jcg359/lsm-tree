import src.dsa.sst.read as sst_read
import src.dsa.sst.utility as sst_u
from src.dsa.memtable.skip_list import SkipList
import src.dsa.sst.search as sst_search


class LSMTreeSearch:
    def __init__(self, memtable: SkipList, data_root_path: str, max_sst_levels: int, last_file_ids: dict[int, str]):
        self._reader = sst_read.SortedTableReader(data_root_path)

        self._sst = sst_search.SortedTableSearch(self._reader)
        self._memtable = memtable

        self._max_sst_levels = max_sst_levels
        self._last_file_ids = last_file_ids
        if 0 not in self._last_file_ids:
            self._last_file_ids[0] = sst_u.ulid_max()

    def get_last_id(self, level: int):
        if level in self._last_file_ids:
            return self._last_file_ids[level]

        return None

    def update_last_id(self, level: int, last_id: str):
        self._last_file_ids[level] = last_id

    def update_memtable(self, memtable: SkipList):
        self._memtable = memtable

    def search(self, key: str):
        result = self._memtable.search(key)
        if result is not None:
            if result == sst_u.tombstone():
                return None, f"MT{sst_u.tombstone_source()}"
            return result, "MT"

        for i in range(0, self._max_sst_levels + 1):
            last_id = self._last_file_ids[i] if i in self._last_file_ids else sst_u.ulid_min()

            result = self._sst.search(key, i, last_id)
            if result is not None:
                if result == sst_u.tombstone():
                    return None, f"L{i}{sst_u.tombstone_source()}"
                return result, f"L{i}"

        return None, f"L{self._max_sst_levels}"

    def level_counts(self):
        return self._reader.get_level_counts(self._last_file_ids, self._max_sst_levels)

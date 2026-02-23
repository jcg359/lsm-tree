import os
from typing import Any, List, Optional, Tuple

import src.dsa.sst.read as sst_read
import src.dsa.sst.utility as sst_u


class SortedTableSearch:
    """Key lookup across an LSM-style SSTable directory tree.

    Delegates all file I/O to an existing SortedTableReader.
    """

    def __init__(self, reader: sst_read.SortedTableReader):
        self._reader = reader

    def search(self, key: str, level: int, last_id: str = "") -> Optional[Any]:
        """Return the value for *key* at *level*, or None if not found / deleted."""
        level_dir = sst_u.level_dir(self._reader.root_data_path, level)
        if not os.path.exists(level_dir):
            return None

        last_id = last_id if level > 0 else sst_u.ulid_max()
        file_ids = self._reader.list_file_ids(level_dir, last_id)

        if not file_ids:
            return None

        if level == 0:
            return self._search_level_zero(key, level_dir, file_ids)
        else:
            return self._search_level_n(key, level_dir, file_ids)

    # ------------------------------------------------------------------
    # Level-specific search
    # ------------------------------------------------------------------

    def _search_level_zero(self, key: str, level_dir: str, file_ids: List[str]) -> Optional[Any]:
        # Files may overlap; traverse newest-first (descending ULID = descending time).
        # The first file that contains the key (including tombstones) is authoritative.
        for file_id in sorted(file_ids, reverse=True):
            found, value = self._lookup_in_file(key, level_dir, file_id)
            if found:
                return value
        return None

    def _search_level_n(self, key: str, level_dir: str, file_ids: List[str]) -> Optional[Any]:
        # Files have non-overlapping key ranges.
        # Binary-search the file list for the rightmost file whose first_key <= key.
        file_ranges: List[Tuple[str, str]] = []  # (first_key, file_id)
        for file_id in file_ids:
            index = self._reader.read_index(level_dir, file_id)
            if index:
                file_ranges.append((index[0]["first_key"], file_id))

        if not file_ranges:
            return None

        file_ranges.sort()

        lo, hi = 0, len(file_ranges) - 1
        candidate_file_id: Optional[str] = None
        while lo <= hi:
            mid = (lo + hi) // 2
            if file_ranges[mid][0] <= key:
                candidate_file_id = file_ranges[mid][1]
                lo = mid + 1
            else:
                hi = mid - 1

        if candidate_file_id is None:
            return None

        found, value = self._lookup_in_file(key, level_dir, candidate_file_id)
        return value if found else None

    # ------------------------------------------------------------------
    # File-level search: index binary search → seek → block scan
    # ------------------------------------------------------------------

    def _lookup_in_file(self, key: str, folder: str, file_id: str) -> Tuple[bool, Any]:
        """Return (found, value). value is None for tombstones; found is False if key absent."""
        index = self._reader.read_index(folder, file_id)
        if not index or key < index[0]["first_key"]:
            return False, None

        # Binary search: rightmost block whose first_key <= key.
        lo, hi = 0, len(index) - 1
        block_entry = index[0]
        while lo <= hi:
            mid = (lo + hi) // 2
            if index[mid]["first_key"] <= key:
                block_entry = index[mid]
                lo = mid + 1
            else:
                hi = mid - 1

        for record in self._reader.read_block(folder, file_id, block_entry):
            if record["key"] == key:
                return True, record["value"]
            if record["key"] > key:
                break  # records are sorted; we passed the target

        return False, None

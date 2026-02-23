import os
from typing import Iterator, List, Optional, Tuple

import src.dsa.sst.write as sst_write
import src.dsa.sst.read as sst_read
import src.dsa.sst.utility as sst_util


class SortedTableCompactor:
    def __init__(self, root_data_path: str, config: sst_util.SortedTableConfiguration):
        self._root_data_path = root_data_path

        self._config = config
        self._reader = sst_read.SortedTableReader(self._root_data_path)
        self._writer = sst_write.SortedTableWriter(self._root_data_path)

    def compact_level_zero(self, last_l1_id: str) -> Tuple[str, List[str]]:
        l0_dir = self._level_dir(0)
        l1_dir = self._level_dir(1)
        os.makedirs(l1_dir, exist_ok=True)

        merge_l0_id = self._oldest_file_id(0)
        if merge_l0_id is None:
            return None, self._reader.list_file_ids(l1_dir, last_l1_id)

        # key range from index + last block only — no full file load
        l0_range = self._reader.get_key_range(l0_dir, merge_l0_id)
        if l0_range is None:
            return None, self._reader.list_file_ids(l1_dir, last_l1_id)

        l0_min, l0_max = l0_range
        # determine which L1 files overlap using index reads only
        overlapping_file_ids, untouched_file_ids = self._partition_level_files(l1_dir, l0_min, l0_max, last_l1_id)

        # plan how many output files and where to split, using only index reads
        split_keys = self._level_key_splits(l0_dir, merge_l0_id, l1_dir, overlapping_file_ids, 1)

        # merge streams block by block — one block per cursor in memory at a time
        l1_cfg = self._config.for_level(1)
        merged = self._merge_records(l0_dir, merge_l0_id, l1_dir, overlapping_file_ids)
        new_file_ids = self._writer.write_split(1, merged, split_keys, l1_cfg.block_size, l1_cfg.blocks_per_file)

        return merge_l0_id, untouched_file_ids + new_file_ids

    def newest_file_id(self, level: int) -> Optional[str]:
        file_ids = self._read_file_ids(level)
        return sorted(file_ids, reverse=True)[0] if file_ids else None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _level_dir(self, level: int) -> str:
        return sst_util.level_dir(self._root_data_path, level)

    def _read_file_ids(self, level):
        folder = self._level_dir(level)
        file_ids = self._reader.list_file_ids(folder, sst_util.ulid_max())
        return file_ids

    def _oldest_file_id(self, level: int) -> Optional[str]:
        file_ids = self._read_file_ids(level)
        return sorted(file_ids)[0] if file_ids else None

    def _partition_level_files(
        self, to_directory: str, from_key_min: str, from_key_max: str, last_id: str
    ) -> Tuple[List[str], List[str]]:
        overlapping: List[str] = []
        untouched: List[str] = []

        for file_id in self._reader.list_file_ids(to_directory, last_id):
            key_range = self._reader.get_key_range(to_directory, file_id)
            if key_range is None:
                untouched.append(file_id)
                continue
            file_min, file_max = key_range
            if file_min <= from_key_max and file_max >= from_key_min:
                overlapping.append(file_id)
            else:
                untouched.append(file_id)

        return overlapping, untouched

    def _level_key_splits(
        self, from_directory: str, from_file_id: str, to_directory: str, to_file_id: List[str], to_level: int
    ) -> List[str]:
        n = self._config.for_level(to_level).min_files
        if n <= 1:
            return []

        # collect all block first_keys from input indices
        all_keys: List[str] = []
        for block in self._reader.read_index(from_directory, from_file_id):
            all_keys.append(block["first_key"])
        for fid in to_file_id:
            for block in self._reader.read_index(to_directory, fid):
                all_keys.append(block["first_key"])

        if not all_keys:
            return []

        all_keys.sort()
        # pick n-1 evenly-spaced keys as split boundaries
        return sorted(set(all_keys[round(i * len(all_keys) / n)] for i in range(1, n)))

    def _merge_records(
        self,
        from_directory: str,
        from_file_id: str,
        to_directory: str,
        to_file_ids: List[str],
    ) -> Iterator[dict]:
        cursors = []

        c = self._reader.make_cursor(from_directory, from_file_id, 0)
        if c:
            cursors.append(c)
        for fid in to_file_ids:
            c = self._reader.make_cursor(to_directory, fid, 1)
            if c:
                cursors.append(c)

        last_key: Optional[str] = None
        while cursors:
            # always take the lowest key; L0 wins on tie
            min_idx = min(
                range(len(cursors)),
                key=lambda i: (cursors[i].current["key"], cursors[i].priority),
            )
            record = cursors[min_idx].current

            if record["key"] != last_key:
                yield record
                last_key = record["key"]
            # else: same key from a lower-priority source — discard

            if not self._reader.advance_cursor(cursors[min_idx]):
                cursors.pop(min_idx)

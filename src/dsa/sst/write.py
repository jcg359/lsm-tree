import json
import os
import pathlib
from typing import Iterable, Iterator, List, Optional, Tuple

from ulid import ULID

import src.dsa.sst.utility as sst_util


class SortedTableWriter:
    def __init__(self, root_data_path: str):
        self._root_data_path = root_data_path

    def write(self, level: int, block_size: int, records: Iterable[dict]) -> Tuple[str, str]:
        folder = sst_util.level_dir(self._root_data_path, level)
        os.makedirs(folder, exist_ok=True)

        file_id = str(ULID())
        data_path = sst_util.data_path(folder, file_id)
        index_path = sst_util.index_path(folder, file_id)

        index = []
        block_num = 0
        block_record_count = 0
        block_first_key: Optional[str] = None
        block_start_offset = 0

        with open(data_path, "wb") as f:
            for record_num, record in enumerate(records):
                key, value = record["key"], record["value"]
                if record_num % block_size == 0:
                    if block_first_key is not None:
                        index.append(
                            {
                                "block": block_num,
                                "first_key": block_first_key,
                                "offset": block_start_offset,
                                "record_count": block_record_count,
                            }
                        )
                        block_num += 1
                    block_start_offset = f.tell()
                    block_first_key = key
                    block_record_count = 0

                line = (json.dumps({"key": key, "value": value}) + "\n").encode("utf-8")
                f.write(line)
                block_record_count += 1

            if block_first_key is not None:
                index.append(
                    {
                        "block": block_num,
                        "first_key": block_first_key,
                        "offset": block_start_offset,
                        "record_count": block_record_count,
                    }
                )

        with open(index_path, "w", encoding="utf-8") as f:
            for entry in index:
                f.write(json.dumps(entry) + "\n")

        return data_path, file_id

    def preserve_files(self, level: int, file_ids: List[str]) -> str:
        folder = sst_util.level_dir(self._root_data_path, level)

        for file in pathlib.Path(folder).iterdir():
            if not file.is_file() or not file.name.endswith(".index.jsonl"):
                continue
            id = file.name.removesuffix(".index.jsonl")
            if id in file_ids:
                continue

            self.remove_file(level, id)

        file_ids.sort(reverse=True)
        return file_ids[0]

    def remove_file(self, level: int, file_id: str) -> None:
        folder = sst_util.level_dir(self._root_data_path, level)
        for path in (
            sst_util.data_path(folder, file_id),
            sst_util.index_path(folder, file_id),
        ):
            if os.path.exists(path):
                os.remove(path)

    def write_split(
        self, level: int, records: Iterator[dict], split_keys: List[str], block_size: int, max_blocks_per_file: int
    ) -> List[str]:
        # Write records across multiple files; start a new file at each split key
        # and also when the current file reaches max_records_per_file.
        split_idx = 0
        file_ids: List[str] = []
        buffer: List[dict] = []
        max_records_per_file = max_blocks_per_file * block_size

        def flush():
            data_path, _ = self.write(level, block_size, buffer)
            file_ids.append(os.path.basename(data_path)[:-6])
            buffer.clear()

        for record in records:
            while split_idx < len(split_keys) and record["key"] >= split_keys[split_idx]:
                if buffer:
                    flush()
                split_idx += 1
            buffer.append(record)
            if len(buffer) >= max_records_per_file:
                flush()

        if buffer:
            flush()

        return file_ids

import json
import os
from dataclasses import dataclass
from typing import List, Optional, Tuple

import src.dsa.sst.utility as sst_util


@dataclass
class SortedTableCursor:
    records: List[dict]
    pos: int
    priority: int
    folder: str
    file_id: str
    blocks: List[dict]
    block_idx: int

    @property
    def current(self) -> dict:
        return self.records[self.pos]


class SortedTableReader:
    def __init__(self, root_data_path: str):
        self._root_data_path = root_data_path

    @property
    def root_data_path(self) -> str:
        return self._root_data_path

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def get_level_counts(
        self,
        last_ids: dict[int, str],
        max_level: int,
    ):
        result = []
        for i in range(0, max_level + 1):
            ldir = sst_util.level_dir(self.root_data_path, i)
            count = 0
            print(f"lastid for level {i} is {last_ids[i]}")
            for fileid in self.list_file_ids(ldir, last_ids[i]):
                print(f"couting {fileid} for level {i}")
                count += sum([ix["record_count"] for ix in self.read_index(ldir, fileid)])

            result.append({"sst_level": f"L{i}", "key_count": count})
        return result

    def list_file_ids(self, folder: str, last_id: str) -> List[str]:
        if not os.path.exists(folder):
            return []
        ids = [
            name[:-6]  # strip ".jsonl"
            for name in os.listdir(folder)
            if name.endswith(".jsonl") and not name.endswith(".index.jsonl")
        ]

        last_id = last_id or ""
        last_id = (sst_util.ulid_min() if last_id == "" else last_id).strip()
        return [fid for fid in ids if fid <= last_id]

    def read_index(self, folder: str, file_id: str) -> List[dict]:
        with open(sst_util.index_path(folder, file_id), "r", encoding="utf-8") as f:
            return [json.loads(line) for line in f if line.strip()]

    def read_block(self, folder: str, file_id: str, block: dict) -> List[dict]:
        data_path = sst_util.data_path(folder, file_id)
        records = []
        with open(data_path, "rb") as f:
            f.seek(block["offset"])
            for _ in range(block["record_count"]):
                line = f.readline()
                if line:
                    records.append(json.loads(line.decode("utf-8")))
        return records

    def get_key_range(self, folder: str, file_id: str) -> Optional[Tuple[str, str]]:
        # first_key from the index; last key requires reading the final block
        blocks = self.read_index(folder, file_id)
        if not blocks:
            return None
        file_min = blocks[0]["first_key"]
        last_block_records = self.read_block(folder, file_id, blocks[-1])
        file_max = last_block_records[-1]["key"] if last_block_records else file_min
        return file_min, file_max

    def make_cursor(self, folder: str, file_id: str, priority: int) -> Optional[SortedTableCursor]:
        blocks = self.read_index(folder, file_id)
        if not blocks:
            return None
        first_block = self.read_block(folder, file_id, blocks[0])
        if not first_block:
            return None
        return SortedTableCursor(
            records=first_block,
            pos=0,
            priority=priority,
            folder=folder,
            file_id=file_id,
            blocks=blocks,
            block_idx=0,
        )

    def advance_cursor(self, cursor: SortedTableCursor) -> bool:
        if cursor.pos + 1 < len(cursor.records):
            cursor.pos += 1
            return True
        # current block exhausted — load the next one
        next_block_idx = cursor.block_idx + 1
        if next_block_idx >= len(cursor.blocks):
            return False
        next_block = self.read_block(cursor.folder, cursor.file_id, cursor.blocks[next_block_idx])
        if not next_block:
            return False
        cursor.records = next_block
        cursor.pos = 0
        cursor.block_idx = next_block_idx
        return True

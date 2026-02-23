import json
import os

import src.dsa.sst.utility as sst_u


class WriteAheadLog:
    def __init__(self, data_root_path: str):
        l0_dir = sst_u.level_dir(data_root_path, 0)
        os.makedirs(l0_dir, exist_ok=True)
        self._path = os.path.join(l0_dir, "wal.jsonl")

    def append(self, key: str, value) -> None:
        with open(self._path, "a", encoding="utf-8") as f:
            f.write(json.dumps({"key": key, "value": value}) + "\n")

    def delete(self) -> None:
        if os.path.exists(self._path):
            os.remove(self._path)

    @property
    def path(self) -> str:
        return self._path

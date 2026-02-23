import os


def level_dir(root_path: str, level: int) -> str:
    return os.path.join(root_path, f"L{level}")


def data_path(folder: str, file_id: str) -> str:
    return os.path.join(folder, f"{file_id}.jsonl")


def index_path(folder: str, file_id: str) -> str:
    return os.path.join(folder, f"{file_id}.index.jsonl")


def tombstone():
    return "__TOMBSTONE__"


def tombstone_source():
    return "-x"


def ulid_max():
    return "ZZZZZZZZZZZZZZZZZZZZZZZZZZ"


def ulid_min():
    return "00000000000000000000000000"


class SortedLevelConfiguration:
    def __init__(self, block_size: int = 10, blocks_per_file: int = 20, min_files: int = 2):
        self.block_size = block_size
        self.blocks_per_file = blocks_per_file
        self.min_files = min_files


class SortedTableConfiguration:
    def __init__(self, levels: dict[int, SortedLevelConfiguration]):
        # levels: mapping of level number (int) -> SortedLevelConfiguration
        self._levels = levels

    def for_level(self, level: int) -> SortedLevelConfiguration:
        return self._levels[level]

import src.dsa.sst.compact as sst_compact
import src.dsa.sst.utility as sst_u
import src.dsa.sst.write as sst_write


class LSMTreeCompator:
    def __init__(self, data_root_path: str):
        # only compacting 1 level for demo purposes
        l1_only_config = sst_u.SortedTableConfiguration(levels={1: sst_u.SortedLevelConfiguration()})
        self._compactor = sst_compact.SortedTableCompactor(
            root_data_path=data_root_path,
            config=l1_only_config,
        )

        self._writer = sst_write.SortedTableWriter(data_root_path)

    def compact_level_zero(self, last_l1_id: str):
        removed_l0_id, surviving_l1_ids = self._compactor.compact_level_zero(last_l1_id)
        self._writer.remove_file(0, removed_l0_id)
        return self._writer.preserve_files(1, surviving_l1_ids)

    def newest_file_id(self, level: int):
        return self._compactor.newest_file_id(level)

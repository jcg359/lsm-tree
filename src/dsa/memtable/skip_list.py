import os
import random
from typing import List, Optional

import src.dsa.sst.write as sst_write
import src.dsa.sst.utility as sst_u


class SkipListNode:
    def __init__(self, key, value, level: int):
        self.key = key
        self.value = value
        self.forward: List[Optional["SkipListNode"]] = [None] * (level + 1)


class SkipList:
    def __init__(self, max_level: int = 3, block_size: int = 10):
        # root node before all real keys
        self._head = SkipListNode(None, None, max_level)
        self._level = 0  # highest level currently in use
        self._size = 0  # count of live (non-tombstoned) entries
        self.block_size = block_size
        self.max_level = max_level

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def insert(self, key, value) -> None:
        update = self._find_update_nodes(key)

        candidate = update[0].forward[0]
        if candidate is not None and candidate.key == key:
            # revive the tombstone
            if candidate.value == sst_u.tombstone():
                self._size += 1
            candidate.value = value
            return

        new_level = self._random_level()

        # levels lazily extend to MAX_LEVEL
        if new_level > self._level:
            for i in range(self._level + 1, new_level + 1):
                update[i] = self._head
            self._level = new_level

        node = SkipListNode(key, value, new_level)
        for i in range(new_level + 1):
            # akin to inserting into ordered linked list
            node.forward[i] = update[i].forward[i]
            update[i].forward[i] = node

        self._size += 1

    def search(self, key):
        node = self._head
        for i in range(self._level, -1, -1):
            # akin to search in ordered linked list, stop if you overshoot
            while node.forward[i] is not None and node.forward[i].key < key:
                node = node.forward[i]

        candidate = node.forward[0]
        if candidate is not None and candidate.key == key:
            return candidate.value
        return None

    def delete(self, key) -> tuple:
        update = self._find_update_nodes(key)

        candidate = update[0].forward[0]
        if candidate is not None and candidate.key == key:
            # key exists - decrement size only if it was a live entry
            if candidate.value != sst_u.tombstone():
                self._size -= 1
            candidate.value = sst_u.tombstone()
            return key, sst_u.tombstone()

        # key not present - insert a tombstone node so the delete propagates to SSTables
        new_level = self._random_level()
        if new_level > self._level:
            for i in range(self._level + 1, new_level + 1):
                update[i] = self._head
            self._level = new_level

        node = SkipListNode(key, sst_u.tombstone(), new_level)
        for i in range(new_level + 1):
            node.forward[i] = update[i].forward[i]
            update[i].forward[i] = node

        return key, sst_u.tombstone()

    def count(self) -> int:
        return self._size

    def flush_to_level_zero(self, root_folder: str) -> tuple[str, str]:
        # walk level-0 linked list and stream records into a new SSTable
        node = self._head.forward[0]

        def _records():
            n = node
            while n is not None:
                yield {"key": n.key, "value": n.value}
                n = n.forward[0]

        return sst_write.SortedTableWriter(root_folder).write(0, self.block_size, _records())

    def ordered_keys(self):
        # walk level-0 linked list in sorted order, skipping tombstones
        node = self._head.forward[0]
        while node is not None:
            if node.value != sst_u.tombstone():
                yield node.key
            node = node.forward[0]

    def __str__(self) -> str:
        pairs = ", ".join(f"{k!r}: {v!r}" for k, v in self.ordered_nodes())
        return f"SkipList({{{pairs}}})"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _find_update_nodes(self, key) -> list:
        # update[i] = rightmost node at level i whose key < key (or head sentinel)
        update = [None] * (self.max_level + 1)
        node = self._head
        for i in range(self._level, -1, -1):
            while node.forward[i] is not None and node.forward[i].key < key:
                node = node.forward[i]
            update[i] = node
        return update

    def _random_level(self) -> int:
        level = 0
        # only half of items will advance from level n to n+1
        while random.random() < 0.5 and level < self.max_level:
            level += 1
        return level

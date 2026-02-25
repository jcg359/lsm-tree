import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.demo.versions import LogSequenceIssuer
from src.demo.controller import LSMController


def test_lsm_controller():
    test_data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

    lsns = LogSequenceIssuer()
    ctrl = LSMController(data_path=test_data_path, lsn_issuer=lsns)

    # 1. truncate with Y
    ctrl.truncate("Y")
    counts = ctrl.level_counts()
    l1_count = sum((int(c["key_count"]) for c in counts))
    assert l1_count == 0, "Expected keys in memtable after truncate"

    # 2. load 50 for 3 customers; get a key from the memtable
    ctrl.load("3", 50)
    keys = ctrl.memtable_keys()
    assert len(keys) > 1, "Expected keys in memtable after load"
    target_key = keys[0]
    l1_key = keys[1]

    # 3. search and confirm result is in MT
    result, source = ctrl.search(target_key)
    [custid, raw_result_input] = target_key.split("#")

    assert result is not None, f"Key {target_key!r} not found after MT load"
    assert source == "MT", f"Expected source MT, got {source!r}"

    # 3a. delete and confirm key is gone; undelete and confirm it comes back
    saved_result = result
    raw_result_input += f",{result['temperature']}{result['scale']},{result['humidity']}"

    ctrl.delete(target_key)
    deleted_result, source = ctrl.search(target_key)
    print(deleted_result)
    assert deleted_result is None, f"Key {target_key!r} should be absent after delete (MT)"
    ctrl.save(custid, raw_result_input)
    restored_result, _ = ctrl.search(target_key)
    assert restored_result is not None, f"Key {target_key!r} should be present after undelete (MT)"

    # 4. keep loading 50 for 3 customers until L0 key count exceeds 150
    while True:
        ctrl.load("3", 50)
        counts = ctrl.level_counts()
        l0_count = next((c["key_count"] for c in counts if c["lsm_level"] == "L0"), 0)
        if l0_count > 150:
            break

    # 5. search and confirm result is now in L0
    result, source = ctrl.search(target_key)
    assert result is not None, f"Key {target_key!r} not found after L0 flush"
    assert source == "L0", f"Expected source L0, got {source!r}"

    # 5a. delete and confirm key is gone; undelete and confirm it comes back
    saved_result = result
    ctrl.delete_input(["", target_key])
    deleted_result, _ = ctrl.search(target_key)
    assert deleted_result is None, f"Key {target_key!r} should be absent after delete (L0)"
    ctrl.save(custid, raw_result_input)
    restored_result, _ = ctrl.search(target_key)
    assert restored_result is not None, f"Key {target_key!r} should be present after undelete (L0)"

    # 6. compact
    ctrl.compact()

    # 7. confirm L1 has items
    counts = ctrl.level_counts()
    l1_count = next((c["key_count"] for c in counts if c["lsm_level"] == "L1"), 0)
    assert l1_count > 0, "Expected items in L1 after compact"

    # 8. search and confirm result is now in L1
    result, source = ctrl.search(l1_key)
    assert result is not None, f"Key {l1_key} not found after compact"
    assert source == "L1", f"Expected source L1, got {source}"

    # 8a. delete and confirm key is gone; undelete and confirm it comes back
    ctrl.delete_input(["", target_key])
    deleted_result, _ = ctrl.search(target_key)
    assert deleted_result is None, f"Key {target_key!r} should be absent after delete (L1)"
    ctrl.save(custid, raw_result_input)
    restored_result, _ = ctrl.search(target_key)
    assert restored_result is not None, f"Key {target_key!r} should be present after undelete (L1)"

    # 9. confirm exactly 2 L1 data files in the data directory
    l1_dir = os.path.join(test_data_path, "L1")
    l1_files = [f for f in os.listdir(l1_dir) if f.endswith(".jsonl") and not f.endswith(".index.jsonl")]
    assert len(l1_files) == 2, f"Expected 2 L1 files, found {len(l1_files)}: {l1_files}"

    # 10. confirm nonzero L0 data files remain
    l0_dir = os.path.join(test_data_path, "L0")
    l0_files = [
        f for f in os.listdir(l0_dir) if f.endswith(".jsonl") and not f.endswith(".index.jsonl") and f != "wal.jsonl"
    ]
    assert len(l0_files) > 0, "Expected at least one L0 file to remain after compact"

    # 11. new controller instance restores the same MT count via WAL
    original_mt_count = next(
        (c["key_count"] for c in ctrl.level_counts(memtable_only=True) if c["lsm_level"] == "MT"), 0
    )

    ctrl2 = LSMController(data_path=test_data_path, lsn_issuer=lsns)
    ctrl2.restore_memtable_wal()

    new_mt_count = next((c["key_count"] for c in ctrl2.level_counts(memtable_only=True) if c["lsm_level"] == "MT"), 0)
    assert new_mt_count == original_mt_count, f"New controller MT count {new_mt_count} != original {original_mt_count}"

    # cleanup
    ctrl.truncate("Y")
    counts = ctrl.level_counts()
    l1_count = sum((int(c["key_count"]) for c in counts))
    assert l1_count == 0, "Expected keys in memtable after truncate"


if __name__ == "__main__":
    test_lsm_controller()
    print("ALL ASSERTIONS PASSED")

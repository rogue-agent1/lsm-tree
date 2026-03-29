#!/usr/bin/env python3
"""Log-Structured Merge Tree (simplified in-memory)."""

class SSTable:
    def __init__(self, data: dict):
        self.data = dict(sorted(data.items()))
        self.keys = sorted(data.keys())

    def get(self, key):
        lo, hi = 0, len(self.keys) - 1
        while lo <= hi:
            mid = (lo + hi) // 2
            if self.keys[mid] == key:
                v = self.data[key]
                return v if v is not None else None
            elif self.keys[mid] < key:
                lo = mid + 1
            else:
                hi = mid - 1
        return None

    def __len__(self):
        return len(self.keys)

class LSMTree:
    def __init__(self, memtable_size: int = 10):
        self.memtable_size = memtable_size
        self.memtable = {}
        self.sstables = []

    def put(self, key: str, value):
        self.memtable[key] = value
        if len(self.memtable) >= self.memtable_size:
            self._flush()

    def get(self, key: str):
        if key in self.memtable:
            v = self.memtable[key]
            return v if v is not "__TOMBSTONE__" else None
        for sst in reversed(self.sstables):
            v = sst.get(key)
            if v is not None:
                return v if v != "__TOMBSTONE__" else None
        return None

    def delete(self, key: str):
        self.memtable[key] = "__TOMBSTONE__"
        if len(self.memtable) >= self.memtable_size:
            self._flush()

    def _flush(self):
        if self.memtable:
            self.sstables.append(SSTable(dict(self.memtable)))
            self.memtable = {}

    def compact(self):
        if len(self.sstables) < 2:
            return
        merged = {}
        for sst in self.sstables:
            merged.update(sst.data)
        merged = {k: v for k, v in merged.items() if v != "__TOMBSTONE__"}
        self.sstables = [SSTable(merged)] if merged else []

    @property
    def size(self):
        return len(self.memtable) + sum(len(s) for s in self.sstables)

def test():
    lsm = LSMTree(5)
    for i in range(20):
        lsm.put(f"key{i:03d}", f"val{i}")
    assert lsm.get("key000") == "val0"
    assert lsm.get("key019") == "val19"
    assert lsm.get("missing") is None
    assert len(lsm.sstables) >= 1
    # Update
    lsm.put("key000", "updated")
    assert lsm.get("key000") == "updated"
    # Delete
    lsm.delete("key005")
    assert lsm.get("key005") is None
    # Compact
    lsm.compact()
    assert lsm.get("key010") == "val10"
    assert lsm.get("key005") is None
    print("  lsm_tree: ALL TESTS PASSED")

if __name__ == "__main__":
    test()

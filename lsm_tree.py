#!/usr/bin/env python3
"""Simplified LSM-tree key-value store."""

class MemTable:
    def __init__(self, max_size=100):
        self.data = {}
        self.max_size = max_size
    def put(self, key, value):
        self.data[key] = value
    def get(self, key):
        return self.data.get(key)
    def delete(self, key):
        self.data[key] = None  # tombstone
    def is_full(self):
        return len(self.data) >= self.max_size
    def flush(self):
        """Return sorted list of (key, value) pairs."""
        return sorted(self.data.items())

class SSTable:
    def __init__(self, entries):
        self.entries = entries  # sorted list of (key, value)
        self._index = {k: i for i, (k, _) in enumerate(entries)}
    def get(self, key):
        i = self._index.get(key)
        if i is not None:
            return self.entries[i][1]
        return KeyError

class LSMTree:
    def __init__(self, memtable_size=100):
        self.memtable = MemTable(memtable_size)
        self.sstables = []  # newest first
        self._size = 0

    def put(self, key, value):
        self.memtable.put(key, value)
        self._size += 1
        if self.memtable.is_full():
            self._flush()

    _TOMBSTONE = object()

    def get(self, key, default=None):
        # Check memtable first
        if key in self.memtable.data:
            v = self.memtable.data[key]
            return default if v is None else v
        # Check SSTables newest to oldest
        for sst in self.sstables:
            v = sst.get(key)
            if v is not KeyError:
                return default if v is None else v
        return default

    def delete(self, key):
        self.memtable.delete(key)

    def _flush(self):
        entries = self.memtable.flush()
        self.sstables.insert(0, SSTable(entries))
        self.memtable = MemTable(self.memtable.max_size)

    def compact(self):
        """Merge all SSTables into one."""
        if len(self.sstables) < 2:
            return
        merged = {}
        for sst in reversed(self.sstables):
            for k, v in sst.entries:
                merged[k] = v
        # Remove tombstones
        entries = sorted((k, v) for k, v in merged.items() if v is not None)
        self.sstables = [SSTable(entries)] if entries else []

if __name__ == "__main__":
    lsm = LSMTree(memtable_size=5)
    for i in range(20):
        lsm.put(f"key_{i:03d}", f"value_{i}")
    print(f"SSTables: {len(lsm.sstables)}")
    print(f"Get key_010: {lsm.get('key_010')}")

def test():
    lsm = LSMTree(memtable_size=5)
    # Insert enough to trigger flushes
    for i in range(20):
        lsm.put(i, i * 10)
    assert len(lsm.sstables) >= 3
    # Read from various levels
    assert lsm.get(0) == 0
    assert lsm.get(19) == 190
    assert lsm.get(999) is None
    assert lsm.get(999, "nope") == "nope"
    # Update
    lsm.put(5, 9999)
    assert lsm.get(5) == 9999
    # Delete
    lsm.delete(10)
    assert lsm.get(10) is None
    # Compact
    lsm.compact()
    assert len(lsm.sstables) <= 1
    assert lsm.get(0) == 0
    assert lsm.get(10) is None  # still deleted
    print("  lsm_tree: ALL TESTS PASSED")

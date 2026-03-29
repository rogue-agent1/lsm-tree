#!/usr/bin/env python3
"""Log-Structured Merge tree (simplified). Zero dependencies."""
import json, os, sys, time

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
        items = sorted(self.data.items())
        self.data.clear()
        return items

class SSTable:
    def __init__(self, items, level=0):
        self.items = items  # sorted list of (key, value)
        self.level = level
        self.index = {k: i for i, (k, _) in enumerate(items)}

    def get(self, key):
        if key in self.index:
            return self.items[self.index[key]][1]
        return None

    def __len__(self):
        return len(self.items)

class LSMTree:
    def __init__(self, memtable_size=100):
        self.memtable = MemTable(memtable_size)
        self.sstables = []

    def put(self, key, value):
        self.memtable.put(key, value)
        if self.memtable.is_full():
            self._flush()

    def get(self, key):
        val = self.memtable.get(key)
        if val is not None: return val if val is not None else None
        if key in self.memtable.data: return None  # tombstone
        for sst in reversed(self.sstables):
            val = sst.get(key)
            if val is not None: return val
        return None

    def delete(self, key):
        self.memtable.delete(key)

    def _flush(self):
        items = self.memtable.flush()
        self.sstables.append(SSTable(items, level=0))
        if len(self.sstables) > 4:
            self._compact()

    def _compact(self):
        merged = {}
        for sst in self.sstables:
            for k, v in sst.items:
                merged[k] = v
        # Remove tombstones
        merged = {k: v for k, v in merged.items() if v is not None}
        self.sstables = [SSTable(sorted(merged.items()), level=1)]

    def stats(self):
        return {"memtable_size": len(self.memtable.data),
                "sstables": len(self.sstables),
                "total_entries": len(self.memtable.data) + sum(len(s) for s in self.sstables)}

if __name__ == "__main__":
    lsm = LSMTree(memtable_size=50)
    for i in range(200):
        lsm.put(f"key_{i:04d}", f"value_{i}")
    print(f"Get key_0100: {lsm.get('key_0100')}")
    print(f"Stats: {lsm.stats()}")

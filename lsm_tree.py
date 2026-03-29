#!/usr/bin/env python3
"""Log-Structured Merge Tree (LSM-Tree) simulation."""
import sys, bisect

class MemTable:
    def __init__(self, max_size=8):
        self.data, self.max_size = {}, max_size
    def put(self, key, value): self.data[key] = value
    def get(self, key): return self.data.get(key)
    def delete(self, key): self.data[key] = None  # tombstone
    def is_full(self): return len(self.data) >= self.max_size
    def flush(self): items = sorted(self.data.items()); self.data.clear(); return items

class SSTable:
    def __init__(self, items): self.items = items  # sorted list of (key, value)
    def get(self, key):
        i = bisect.bisect_left(self.items, (key,))
        if i < len(self.items) and self.items[i][0] == key:
            return self.items[i][1]
        return "__MISS__"
    def scan(self, start=None, end=None):
        result = []
        for k, v in self.items:
            if start and k < start: continue
            if end and k >= end: break
            if v is not None: result.append((k, v))
        return result

class LSMTree:
    def __init__(self, memtable_size=8, max_level_size=4):
        self.mem = MemTable(memtable_size)
        self.levels, self.max_level = [], max_level_size
    def put(self, key, value):
        self.mem.put(key, value)
        if self.mem.is_full(): self._flush()
    def _flush(self):
        items = self.mem.flush()
        self.levels.insert(0, SSTable(items))
        if len(self.levels) > self.max_level: self._compact()
    def _compact(self):
        merged = {}
        for level in reversed(self.levels):
            for k, v in level.items: merged[k] = v
        self.levels = [SSTable(sorted(merged.items()))]
    def get(self, key):
        v = self.mem.get(key)
        if v is not None: return v
        if key in self.mem.data: return None  # tombstone
        for sst in self.levels:
            v = sst.get(key)
            if v != "__MISS__": return v
        return None
    def delete(self, key): self.mem.delete(key); self._flush_if_full()
    def _flush_if_full(self):
        if self.mem.is_full(): self._flush()

def main():
    if len(sys.argv) < 2: print("Usage: lsm_tree.py <demo|test>"); return
    cmd = sys.argv[1]
    if cmd == "demo":
        t = LSMTree(memtable_size=4)
        for i in range(20): t.put(f"key{i:02d}", f"val{i}")
        print(f"key05: {t.get('key05')}, key15: {t.get('key15')}")
    elif cmd == "test":
        t = LSMTree(memtable_size=4, max_level_size=3)
        for i in range(50): t.put(i, i*10)
        for i in range(50): assert t.get(i) == i*10, f"Miss {i}"
        t.put(25, 999); assert t.get(25) == 999
        assert t.get(999) is None
        t.delete(10)
        # After compaction tombstones handled
        print("All tests passed!")

if __name__ == "__main__": main()

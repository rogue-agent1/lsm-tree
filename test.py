from lsm_tree import LSMTree
lsm = LSMTree(memtable_size=10)
for i in range(50):
    lsm.put(f"k{i:03d}", f"v{i}")
assert lsm.get("k025") == "v25"
lsm.put("k025", "updated")
assert lsm.get("k025") == "updated"
lsm.delete("k010")
assert lsm.get("k010") is None
print("LSM tree tests passed")
# Disk format
## SST:
![alt text](sst.png)


# TODO
0. fix tests: File handles not added to LsmDisk.
   - I need mutex or lock-free list for level_s SstReader vector. cannot access &mut.

1. Periodic flush
   1. flush signal
2. Bloom Filter (or some advanced ones)
3. Compact
   1. compact signal

# Disk format

## SST

### Data
![](./images/sst.png)

### Index
![](./images/index.png)

### Bloom filter
```
+-------------------+-------------------+-------------------+-------------------+
|   num_bits (4B)   |      k (4B)       |     bits (var)    |  checksum (4B)    |
+-------------------+-------------------+-------------------+-------------------+
```


## WAL
![](./images/wal.png)


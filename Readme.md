# PCMap

## Persistent-Concurrent Map


## Overview

The `pcmap` repository is an implementation of a concurrent hash array mapped trie that persists to disk using memory mapped files. Most chamts are implemented as in-memory only, which means that while the data structure itself is immutable, the data is volatile and the data structure will not persist on system restarts. This implementation looks to create a persistent version of a chamt so that the data structure is serialized and stored to disk, eliminating the need to rebuild the entire structure if a system restarts.

`pcmap` is inspired by boltdb, which was my introduction into how databases work at a more fundamental level. Boltdb utilizes a memory-mapped file its mechanism to persist the data to disk while also maintaining both high read and write speeds. The difference lies in the data structure used, however. Boltdb utilizes a B+tree as its underlying datastructure, which excels at sequential reads. However, hash array mapped tries excel at random reads and writes, with amortized time complexity of O(1) for both single read and write operations. 

This project is an exploration of memory mapped files and taking a different approach to storing and retrieving data within a database.
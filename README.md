## cask-db

.Net Implementation for BitCask key value store described in [bitcask-intro.pdf](https://riak.com/assets/bitcask-intro.pdf). Can be used as in-process persisted key-value store. Implementation is for learning purpose only and might not be suitable for production usage.

Supports file rotation, merge, synchronous writes and hint files. CRC support, though trivial to add, is not implemented.

Usage examples can be found in `Program.cs` and unit-test project.

**Merge strategy**

Instead of merging all data file on program startup, I chose to go with a more continuous and resumable process, which can be carried out concurrently with the read and writes.

When merge is triggered, the active file is rotated twice, the file produced by first rotation is used as the merge file (which will contain all the entries from the previous data files). File produced by the second rotation is used as the new active file.

Data files before merge

```
001.data
002.data <- active file
```

when merge is triggered

```
001.data
002.data
003.data <- merge file
004.data <- active file
```

The read and writes can continue in the 004.data file, while the background process will compact all the entries from 001.data and 002.data to 003.data file conditionally (skip entries if deleted or if newer entry is already present in 004.data based on the state of in-memory keyDir structure).

**Performance on my local machine for 1 million writes and reads**

```
$ dotnet run -c Release
    write async c=1M: 508.9696 ms
     write sync c=1M: 6245.2902 ms
           read c=1M: 325.8105 ms
         Peak memory: 43.78515625 Mb
```



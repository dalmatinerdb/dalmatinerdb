# DalmatinerDB
DalmatinerDB is a metric database written in pure Erlang. It takes advantage of some special properties of metrics to make some tradeoffs. The goal is to make a store for metric data (time, value of a metric) that is fast, has a low overhead, and is easy to query and manage.

# Tradeoffs
I try to be explict about the tradeoffs made, this way people can decide of they are happy with what they 'lose' and what they 'win'. The acceptable tradeoffs differ from case to case but I hope the set choices made fit a metric store quite well. If you are comparing DalmatinerDB with X please don't assume that just because X does not list the tradeoffs made they have none, be inquisitive and make a decision based on facts not marketing.


## Let the Filesystem handle it
A lot of work is handed down to the file system, ZFS is exceptionally smart and can do things like checksums, compressions and caching very well. Handing down these tasks to the filesystem simplifies the codebase and builds on very well tested and highly performant code instead of trying to reimplement it.

## Integers
All data stored in metric DB is stored as a 56-bit signed integer. Integers are 'lossless' so arithmetic over them will always produce the correct and expected results. 56-bit should offer large enough numbers for most metrics.

For metrics it is enough to have a 'fixed' precisions, so storing higher than increments of 1 precision (i.e. 32.5 degrees celsius) can be simply achieved by scaling the metric to the wanted precision before storing it. This has the added advantage that it makes people think about their data instead of just vomiting it into storage.

While 56-bit sounds like a lot of bits, testing with real world data has shown that the ZFS compression ratio of written metrics is > 6x (unwritten metrics compress better). This means the effective size is about 11 bit per metricpoint.

## No guarantee of storage
DalmatinerDB offers a 'best effort' on storing the metrics, there is no log for writes (there is the ZIL if enabled in ZFS) or forced sync after each write. This means that if your network fails packets can get lost, if your server crashes unwritten data can be lost.

The point is that losing one or two metric points in a huge series is a non-problem, the importance of a metric is often seen in aggregate and DalmatinerDB fills in the blanks with the last written value. However there is explictly no guarantee that data is written, this can be an issue if every single point of metric is of importance!

## Flat files
Data is stored in a flat binary format, this means that reads and writes can be calculated to a filename+offset by simple math, there is no need for transversing datastructures. This means however that if a metric stops unwritten points can 'linger' around for a while depending on how the file size was picked.

As an example if metrics are stored with second precision and 1 week of data is stored per file up to one week of unused data can be stored but it should be taken into account that with compression this data will be compressed quite well.

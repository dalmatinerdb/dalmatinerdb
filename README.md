Official Site: https://dalmatiner.io/

# DalmatinerDB
DalmatinerDB is a metric database written in pure Erlang. It takes advantage of some special properties of metrics to make some tradeoffs. The goal is to make a store for metric data (time, value of a metric) that is fast, has a low overhead, and is easy to query and manage.

# Tradeoffs
I try here to be explict about the tradeoffs we made, so people can decide if they are happy with them (costs vs gains). The acceptable tradeoffs differ from case to case, but I hope the choices we made fit a metric store quite well. If you are comparing DalmatinerDB with X, please don't assume that just because X does not list the tradeoffs they made, they have none; be inquisitive and make a decision based on facts, not marketing.

A detailed comparison between databases can be found here:

[https://docs.google.com/spreadsheets/d/1sMQe9oOKhMhIVw9WmuCEWdPtAoccJ4a-IuZv4fXDHxM/edit#gid=0](https://docs.google.com/spreadsheets/d/1sMQe9oOKhMhIVw9WmuCEWdPtAoccJ4a-IuZv4fXDHxM/edit#gid=0)

## Let the Filesystem handle it
A lot of work is handed down to the file system, ZFS is exceptionally smart and can do things like checksums, compressions and caching very well. Handing down these tasks to the filesystem simplifies the codebase, and builds on very well tested and highly performant code, instead of trying to reimplement it.

## No guarantee of storage
DalmatinerDB offers a 'best effort' on storing the metrics, there is no log for writes (if enabled in ZFS, the ZIL (ZFS Intent Log) can log write operations) or forced sync after each write. This means that if your network fails, packets can get lost, and if your server crashes, unwritten data can be lost.

The point is that losing one or two metric points in a huge series is a non-problem, the importance of a metric is often seen in aggregates, and DalmatinerDB fills in the blanks with the last written value. However there is explictly no guarantee that data is written, so *this can be an issue if every single point of metric is of importance!*

## Flat files
Data is stored in a flat binary format, this means that reads and writes can be calculated to a filename+offset by simple math, there is no need for traversing datastructures. This means however that if a metric stops unwritten, points can 'linger' around for a while depending on how the file size was picked.

As an example: if metrics are stored with a precision down to the second, and 1 week of data is stored per file, up to one week of unused data can be stored, but it should be taken into account that with compression this data will be compressed quite well.

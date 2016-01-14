# DalmatinerDB performance measurement with different ZFS setup

## Environment

Following tests are run with *cache_points* = 600

Server *ddbn2* has zfs pool on single ssd disk.

Server *ddbn3* has zfs pool on 2 striped spinning disks with ssd cache.

## Measurement

All measurements are taken at least 10 minutes after all hagger agents are started to make sure all buffers are saturated.

*Time* is a time when measurements are taken. You can use it to lookup full history.

*Metrics* Is total number of metrics instrumented all haggar instances should be running across all instances.

### Tests at 14th Jan 2016

| Server |     Time | Met [k] | Mputs | 99p [ms] | CPU [%] | Write IO | Read IO | Errors  |
| ------ |     ---- | ------- | ----- | -------- | ------- | -------- | ------- | ------  |
| ddbn2  | 11:20:15 |     300 |  5672 |      178 |      74 |     1269 |     372 | unknown |
| ddbn2  | 11:42:30 |     400 |  6740 |      238 |      75 |     2326 |     842 | unknown |
| ddbn2  | 12:25:00 |     300 |  5747 |      161 |      74 |      897 |     366 | clear   |
| ddbn3  | 12:04:30 |     200 |  4342 |      107 |      68 |      416 |       0 | clear   |
| ddbn3  | 12:31:00 |     300 |  5655 |      140 |      79 |      441 |     638 | clear   |

*NOTE* IO is very spiky, so probably best to compare on graphs. Measurements are for that reason in spikes.

*NOTE 2* Only 4 hagger porcesses, each producing 100k metrics per second can be run on single metric box. If oyu run more they start competing for resources and it becomes hard to tell how many metrics is actually pushed.

# DalmatinerDB performance measurement with different ZFS setup

## Environment

Following tests are run with *cache_points* = 600

Server *ddbn2* has zfs pool on single ssd disk.

Server *ddbn3* has zfs pool on 2 striped spinning disks with ssd cache.

## Measurement

All measurements are taken at least 10 minutes after all hagger agents are started to make sure all buffers are saturated.

*Time* is a time when measurements are taken. You can use it to lookup full history.

*Metrics* Is total number of metrics instrumented all haggar instances should be running across all instances.

### Tests on SSD at 14th Jan 2016

| Server |     Time | Met [k] | Mputs | 99p [us] | CPU [%] | Write IO | Read IO | Errors  |
| ------ |     ---- | ------- | ----- | -------- | ------- | -------- | ------- | ------  |
| ddbn2  | 11:20:15 |     300 |  5672 |      178 |      74 |     1269 |     372 | unknown |
| ddbn2  | 11:42:30 |     400 |  6740 |      238 |      75 |     2326 |     842 | unknown |
| ddbn2  | 12:25:00 |     300 |  5747 |      161 |      74 |      897 |     366 | clear   |
| ddbn2  | 14:44:00 |     400 |  6102 |      202 |      74 |     2605 |    1556 | clear   |
| ddbn2  | 15:08:30 |     500 |  8700 |      291 |      80 |     3168 |     971 | clear   |
| ddbn2  | 15:56:00 |     600 | 12120 |      611 |      83 |     3154 |     844 | clear   |
| ddbn2  | 16:50:00 |     800 | 14406 |     1127 |      87 |     6013 |    1178 | clear   |


### Tests on 7200RPM stripe at 14th Jan 2016

| Server |     Time | Met [k] | Mputs | 99p [us] | CPU [%] | Write IO | Read IO | Errors        |
| ------ |     ---- | ------- | ----- | -------- | ------- | -------- | ------- | ------        |
| ddbn3  | 12:04:30 |     200 |  4342 |      107 |      68 |      416 |       0 | clear         |
| ddbn3  | 12:31:00 |     300 |  5655 |      140 |      79 |      441 |     638 | clear         |
| ddbn3  | 13:26:14 |     400 |  6795 |     6796 |      83 |      229 |     617 | vnode timeout |
| ddbn3  | 14:36:00 |     400 |   300 | 70000000 |      70 |      960 |     224 | all the time  |


IO is very spiky, so probably best to compare on graphs. Measurements are for that reason in spikes.

Only 4 hagger porcesses, each producing 100k metrics per second can be run on single metric box. If oyu run more they start competing for resources and it becomes hard to tell how many metrics is actually pushed.

When ddb is overloaded on IO, it will record errors like:

    metric_vnode command failed {timeout,{gen_server,call,[<0.1183.0>,{write,...}]}}
    
After that server becomes unstable and never truly recovers !!!

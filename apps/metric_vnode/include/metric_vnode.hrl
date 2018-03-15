-define(DT_MVNODE_CACHE_EVICT_EVENT, 4501).
-define(DT_MSTORE_READ_RETURN, 4402).
-define(DT_MSTORE_WRITE_ENTRY, 4411).
-define(DT_MSTORE_WRITE_RETURN, 4412).


-define(DT_ENTRY, 1).
-define(DT_RETURN, 2).

-define(DT_READ, 1).
-define(DT_WRITE, 2).

-define(DT_CACHE_EVICT(Size, Chunks),
        dyntrace:p(?DT_MVNODE_CACHE_EVICT_EVENT, Size, Chunks)).

-define(DT_READ_RETURN,
        dyntrace:p(?DT_MSTORE_READ_RETURN)).

-define(DT_WRITE_ENTRY(Metric, Time, Size),
        dyntrace:p(?DT_MSTORE_WRITE_ENTRY, Time, Size, [Metric])).

-define(DT_WRITE_RETURN,
        dyntrace:p(?DT_MSTORE_WRITE_RETURN)).

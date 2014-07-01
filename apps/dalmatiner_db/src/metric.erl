-module(metric).

-export([
         put/4,
         mput/2,
         get/4,
         list/0,
         list/1
        ]).

-ignore_xref([put/4]).


mput(Nodes, Acc) ->
    dict:fold(fun(DocIdx, Data, ok) ->
                      do_mput(orddict:fetch(DocIdx, Nodes), Data, 1);
                 (DocIdx, Data, R) ->
                      do_mput(orddict:fetch(DocIdx, Nodes), Data, 1),
                      R
              end, ok, Acc).

put(Bucket, Metric, Time, Value) ->
    do_put(Bucket, Metric, Time, Value, 1, 1).

get(Bucket, Metric, Time, Count) ->
    dalmatiner_read_fsm:start({metric_vnode, metric}, get, {Bucket, Metric}, {Time, Count}).

list() ->
    metric_coverage:start(list).

list(Bucket) ->
    metric_coverage:start({metrics, Bucket}).

do_put(Bucket, Metric, Time, Value, N, W) ->
    DocIdx = riak_core_util:chash_key({Bucket, Metric}),
    Preflist = riak_core_apl:get_apl(DocIdx, N, metric),
    ReqID = make_ref(),
    metric_vnode:put(Preflist, ReqID, Bucket, Metric, {Time, Value}),
    do_wait(W, ReqID).


do_mput(Preflist, Data, W) ->
    ReqID = make_ref(),
    metric_vnode:mput(Preflist, ReqID, Data),
    do_wait(W, ReqID).

do_wait(0, _ReqID) ->
    ok;

do_wait(W, ReqID) ->
    receive
        {ReqID, ok} ->
            do_wait(W - 1, ReqID)
    after
        1000 ->
            {error, timeout}
    end.

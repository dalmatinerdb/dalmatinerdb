-module(metric).
-include_lib("riak_core/include/riak_core_vnode.hrl").
-export([
         put/3,
         mput/2,
         get/3,
         list/0
        ]).
-ignore_xref([
              put/3,
              get/3,
              list/0
             ]).


mput(Nodes, Acc) ->
    dict:map(fun(DocIdx, Data) ->
                     do_mput(orddict:fetch(DocIdx, Nodes), Data, 1)
             end, Acc).

put(Metric, Time, Value) ->
    do_put(Metric, Time, Value, 1, 1).

get(Metric, Time, Count) ->
    metric_read_fsm:start({metric_vnode, metric}, get, Metric, {Time, Count}).

list() ->
    metric_coverage:start(metrics).

do_put(Metric, Time, Value, N, W) ->
    DocIdx = riak_core_util:chash_key({<<"metric">>, Metric}),
    Preflist = riak_core_apl:get_apl(DocIdx, N, metric),
    ReqID = make_ref(),
    metric_vnode:put(Preflist, ReqID, Metric, {Time, Value}),
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

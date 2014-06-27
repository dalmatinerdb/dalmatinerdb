-module(metric).
-include_lib("riak_core/include/riak_core_vnode.hrl").
-export([
         ping/0,
         put/3,
         mput/1,
         get/3,
         list/0
        ]).
-ignore_xref([
              ping/0,
              put/3,
              get/3,
              list/0
             ]).
%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, metric),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, metric_vnode_master).


mput(Acc) ->
    dict:map(fun(DocIdx, Data) ->
                     do_mput(DocIdx, Data, 1, 1)
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
    metric_vnode:put(Preflist, {ReqID, node()}, Metric, {Time, Value}),
    do_wait(W, ReqID).


do_mput(DocIdx, Data, N, W) ->
    Preflist = riak_core_apl:get_apl(DocIdx, N, metric),
    ReqID = make_ref(),
    metric_vnode:mput(Preflist, {ReqID, node()}, Data),
    do_wait(W, ReqID).

do_wait(0, _ReqID) ->
    ok;

do_wait(W, ReqID) ->
    receive
        {ok, ReqID} ->
            do_wait(W - 1, ReqID)
    after
        1000 ->
            {error, timeout}
    end.

-module(metric).
-include_lib("riak_core/include/riak_core_vnode.hrl").
-export([
         ping/0,
         put/3,
         get/3
        ]).
-ignore_xref([
         ping/0,
         put/3,
         get/3
        ]).
%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, metric),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, metric_vnode_master).

put(Metric, Time, Value) ->
    metric_write_fsm:write({metric_vnode, metric}, put, {Metric, Time}, Value).

get(Metric, Time, Count) ->
    metric_read_fsm:start({metric_vnode, metric}, get, {Metric, Time}, Count).

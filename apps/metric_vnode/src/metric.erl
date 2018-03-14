-module(metric).

-export([
         put/4,
         mput/5,
         mput_trie/5,
         mput_list/5,
         get/6,
         get/7,
         delete/1,
         list/1,
         list/2,
         update_ttl/2,
         update_env/0,
         cache_stats/0
        ]).

-ignore_xref([update_ttl/2, get/6, put/4, update_env/0]).


mput(Nodes, Bucket, Acc, W, N) ->
    ddb_histogram:timed_update(
      mput, dict, fold,
      [fun(DocIdx, _Data, R) when not is_integer(DocIdx) ->
               R;
          (DocIdx, Data, ok) ->
               do_mput(orddict:fetch(DocIdx, Nodes), Bucket, Data, W, N);
          (DocIdx, Data, R) ->
               do_mput(orddict:fetch(DocIdx, Nodes), Bucket, Data, W, N),
               R
       end, ok, Acc]).

mput_trie(Nodes, Bucket, Acc, W, N) ->
    ddb_histogram:timed_update(
      mput, btrie, fold,
      [fun(<<DocIdx:160>>, Data, ok) ->
               do_mput(orddict:fetch(DocIdx, Nodes), Bucket, Data, W, N);
          (<<DocIdx:160>>, Data, R) ->
               do_mput(orddict:fetch(DocIdx, Nodes), Bucket, Data, W, N),
               R;
          (_, _, R) ->
               R
       end, ok, Acc]).

mput_list(Nodes, Bucket, Acc, W, N) ->
    ddb_histogram:timed_update(
      mput, lists, foldl,
      [fun({DocIdx, _Data}, R) when not is_integer(DocIdx) ->
               R;
          ({DocIdx, Data}, ok) ->
               do_mput(orddict:fetch(DocIdx, Nodes), Bucket, Data, W, N);
          ({DocIdx, Data}, R) ->
               do_mput(orddict:fetch(DocIdx, Nodes), Bucket, Data, W, N),
               R
       end, ok, Acc]).

put(Bucket, Metric, Time, Value) ->
    {ok, N} = application:get_env(dalmatiner_db, n),
    {ok, W} = application:get_env(dalmatiner_db, w),
    PPF = dalmatiner_opt:ppf(Bucket),
    ddb_histogram:timed_update(
      put,
      fun() ->
              put(Bucket, Metric, PPF, Time, Value, N, W)
      end).

put(Bucket, Metric, PPF, Time, Value, N, W) ->
    do_put(Bucket, Metric, PPF, Time, Value, N, W).

get(Bucket, Metric, Time, Count, S, Opts) ->
    get(Bucket, Metric, dalmatiner_opt:ppf(Bucket), Time, Count, S, Opts).

get(Bucket, Metric, PPF, Time, Count, _S, Opts) when
      Time div PPF =:= (Time + Count - 1) div PPF->
    ddb_histogram:timed_update(
      get, dalmatiner_read_fsm, start,
      [{metric_vnode, metric},
       get,
       {Bucket, {Metric, Time div PPF}},
       {Time, Count},
       Opts]).

update_ttl(Bucket, TTL) ->
    dalmatiner_opt:set_lifetime(Bucket, TTL),
    metric_coverage:start({update_ttl, Bucket}).

update_env() ->
    metric_coverage:start(update_env).

list(Bucket) ->
    ddb_histogram:timed_update(
      list_metrics, metric_coverage, start, [{metrics, Bucket}]).

delete(Bucket) ->
    R = ddb_histogram:timed_update(
      list_metrics, metric_coverage, start, [{delete, Bucket}]),
    dalmatiner_opt:delete(Bucket),
    R.

list(Bucket, Prefix) ->
    ddb_histogram:timed_update(
      list_metrics, metric_coverage, start, [{metrics, Bucket, Prefix}]).

do_put(Bucket, Metric, PPF, Time, Value, N, W) ->
    DocIdx = riak_core_util:chash_key({Bucket, {Metric, Time div PPF}}),
    Preflist = riak_core_apl:get_apl(DocIdx, N, metric),
    ReqID = make_ref(),
    metric_vnode:put(Preflist, ReqID, Bucket, Metric, {Time, Value}),
    do_wait(W, 0, N, ReqID).

do_mput(Preflist, Bucket, Data, W, N) ->
    ReqID = make_ref(),
    metric_vnode:mput(Preflist, ReqID, Bucket, Data),
    do_wait(W, 0, N, ReqID).


%% We got enough replies to be happy
do_wait(0, _O, _N, _ReqID) ->
    ok;
%% We did not get enough replies but
%% every node did reply eithe either with
%% an overload or a ok.
do_wait(W, O, N, _ReqID) when W + O == N ->
    {error, failed};

do_wait(W, O, N, ReqID) ->
    receive
        {ReqID, ok} ->
            do_wait(W - 1, O, N, ReqID);
        {fail, ReqID, _Idx, overload} ->
            do_wait(W, O + 1, N, ReqID)
    after
        5000 ->
            {error, timeout}
    end.

cache_stats() ->
    Nodes = riak_core_vnode_master:all_nodes(metric_vnode),
    States = [sys:get_state(P) || P <- Nodes],
    Caches = [{element(2, element(2, S)),
               element(4, element(4, element(2, S)))} || S <- States],
    [{I, mcache:stats(C)} || {I, C} <- Caches].




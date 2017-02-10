-module(metric).

-export([
         put/4,
         mput/4,
         get/5,
         get/6,
         delete/1,
         list/1,
         list/2,
         update_ttl/2,
         update_env/0
        ]).

-ignore_xref([update_ttl/2, get/4, put/4, update_env/0]).


mput(Nodes, Acc, W, N) ->
    folsom_metrics:histogram_timed_update(
      mput, dict, fold,
      [fun(DocIdx, Data, ok) ->
               do_mput(orddict:fetch(DocIdx, Nodes), Data, W, N);
          (DocIdx, Data, R) ->
               do_mput(orddict:fetch(DocIdx, Nodes), Data, W, N),
               R
       end, ok, Acc]).

put(Bucket, Metric, Time, Value) ->
    {ok, N} = application:get_env(dalmatiner_db, n),
    {ok, W} = application:get_env(dalmatiner_db, w),
    PPF = dalmatiner_opt:ppf(Bucket),
    folsom_metrics:histogram_timed_update(
      put,
      fun() ->
              put(Bucket, Metric, PPF, Time, Value, N, W)
      end).

put(Bucket, Metric, PPF, Time, Value, N, W) ->
    do_put(Bucket, Metric, PPF, Time, Value, N, W).

get(Bucket, Metric, Time, Count, Opts) ->
    get(Bucket, Metric, dalmatiner_opt:ppf(Bucket), Time, Count, Opts).

get(Bucket, Metric, PPF, Time, Count, Opts) when
      Time div PPF =:= (Time + Count - 1) div PPF->
    folsom_metrics:histogram_timed_update(
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
    folsom_metrics:histogram_timed_update(
      list_metrics, metric_coverage, start, [{metrics, Bucket}]).

delete(Bucket) ->
    R = folsom_metrics:histogram_timed_update(
      list_metrics, metric_coverage, start, [{delete, Bucket}]),
    dalmatiner_opt:delete(Bucket),
    R.

list(Bucket, Prefix) ->
    folsom_metrics:histogram_timed_update(
      list_metrics, metric_coverage, start, [{metrics, Bucket, Prefix}]).

do_put(Bucket, Metric, PPF, Time, Value, N, W) ->
    DocIdx = riak_core_util:chash_key({Bucket, {Metric, Time div PPF}}),
    Preflist = riak_core_apl:get_apl(DocIdx, N, metric),
    ReqID = make_ref(),
    metric_vnode:put(Preflist, ReqID, Bucket, Metric, {Time, Value}),
    do_wait(W, 0, N, ReqID).

do_mput(Preflist, Data, W, N) ->
    ReqID = make_ref(),
    metric_vnode:mput(Preflist, ReqID, Data),
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

-module(metric).

-export([
         put/4,
         mput/3,
         get/4,
         get/5,
         ppf/1,
         list/0,
         list/1,
         list/2
        ]).

-ignore_xref([get/4, put/4]).

-define(WEEK, 604800). %% Seconds in a week.


mput(Nodes, Acc, W) ->
    folsom_metrics:histogram_timed_update(
      mput, dict, fold,
      [fun(DocIdx, Data, ok) ->
               do_mput(orddict:fetch(DocIdx, Nodes), Data, W);
          (DocIdx, Data, R) ->
               do_mput(orddict:fetch(DocIdx, Nodes), Data, W),
               R
       end, ok, Acc]).

put(Bucket, Metric, Time, Value) ->
    {ok, N} = application:get_env(dalmatiner_db, n),
    {ok, W} = application:get_env(dalmatiner_db, w),
    PPF = ppf(Bucket),
    folsom_metrics:histogram_timed_update(
      put,
      fun() ->
              put(Bucket, Metric, PPF, Time, Value, N, W)
      end).

put(Bucket, Metric, PPF, Time, Value, N, W) ->
    do_put(Bucket, Metric, PPF, Time, Value, N, W).

ppf(Bucket) ->
    dalmatiner_opt:get(<<"buckets">>, Bucket,
                       <<"points_per_file">>,
                       {metric_vnode, points_per_file}, ?WEEK).
get(Bucket, Metric, Time, Count) ->
    get(Bucket, Metric, ppf(Bucket), Time, Count).

get(Bucket, Metric, PPF, Time, Count) when
      Time div PPF =:= (Time + Count - 1) div PPF->
    folsom_metrics:histogram_timed_update(
      get, dalmatiner_read_fsm, start,
      [{metric_vnode, metric}, get, {Bucket, {Metric, Time div PPF}},
       {Time, Count}]).

list() ->
    folsom_metrics:histogram_timed_update(
      list_buckets, metric_coverage, start, [list]).

list(Bucket) ->
    folsom_metrics:histogram_timed_update(
      list_metrics, metric_coverage, start, [{metrics, Bucket}]).

list(Bucket, Prefix) ->
    folsom_metrics:histogram_timed_update(
      list_metrics, metric_coverage, start, [{metrics, Bucket, Prefix}]).

do_put(Bucket, Metric, PPF, Time, Value, N, W) ->
    DocIdx = riak_core_util:chash_key({Bucket, {Metric, Time div PPF}}),
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
        5000 ->
            {error, timeout}
    end.

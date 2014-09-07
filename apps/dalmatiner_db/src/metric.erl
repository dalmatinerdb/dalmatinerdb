-module(metric).

-export([
         put/4,
         put/6,
         mput/3,
         get/4,
         list/0,
         list/1
        ]).

-ignore_xref([put/4, put/6]).


mput(Nodes, Acc, W) ->
    ReqIDs =
		dict:fold(fun(DocIdx, Data, Rs) ->
						  R = async_mput(orddict:fetch(DocIdx, Nodes), Data),
						  [R | Rs]
				  end, [], Acc),
	Reqs1 = [do_wait(W, ReqId) || ReqId <- ReqIDs],
	case [R || R <- Reqs1, R /= ok] of
		[] ->
			ok;
		[E | _] ->
			E
	end.

put(Bucket, Metric, Time, Value) ->
    {ok, N} = application:get_env(dalmatiner_db, n),
    {ok, W} = application:get_env(dalmatiner_db, w),
    put(Bucket, Metric, Time, Value, N, W).

put(Bucket, Metric, Time, Value, N, W) ->
    do_put(Bucket, Metric, Time, Value, N, W).

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

async_mput(Preflist, Data) ->
    ReqID = make_ref(),
    metric_vnode:mput(Preflist, ReqID, Data),
	ReqID.

%% do_mput(Preflist, Data, W) ->
%% 	ReqID = async_mput(Preflist, Data),
%%     do_wait(W, ReqID).

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

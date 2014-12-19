-module(dalmatiner_tcp).

-behaviour(ranch_protocol).

-include_lib("dproto/include/dproto.hrl").

-export([start_link/4]).
-export([init/4]).

-record(state,
        {cbin,
         nodes,
         n = 1 :: pos_integer(),
         w = 1 :: pos_integer(),
         fast_loop_count :: pos_integer(),
         wait = 5000 :: pos_integer()
        }).

-record(sstate,
        {cbin,
         nodes,
         n = 1 :: pos_integer(),
         w = 1 :: pos_integer(),
         last = 0 :: non_neg_integer(),
         max_diff = 1 :: pos_integer(),
         wait = 5000 :: pos_integer(),
         dict = dict:new() :: dict(),
         bucket :: binary()}).

start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

init(Ref, Socket, Transport, _Opts = []) ->
    {ok, FLC} = application:get_env(dalmatiner_db, fast_loop_count),
    {ok, Wait} = application:get_env(dalmatiner_db, loop_wait),
    {ok, N} = application:get_env(dalmatiner_db, n),
    {ok, W} = application:get_env(dalmatiner_db, w),
    State = #state{n=N, w=W, fast_loop_count=FLC, wait=Wait},
    ok = Transport:setopts(Socket, [{packet, 4}]),
    ok = ranch:accept_ack(Ref),
    loop(Socket, Transport, State, 0).

loop(Socket, Transport, State = #state{fast_loop_count = FL}, 0) ->
    {ok, CBin} = riak_core_ring_manager:get_chash_bin(),
    Nodes = chash:nodes(chashbin:to_chash(CBin)),
    Nodes1 = [{I, riak_core_apl:get_apl(I, State#state.n, metric)}
              || {I, _} <- Nodes],
    loop(Socket, Transport, State#state{nodes = Nodes1, cbin=CBin}, FL);

loop(Socket, Transport, State, Loop) ->
    case Transport:recv(Socket, 0, State#state.wait) of
        {ok, Data} ->
            case dproto_tcp:decode(Data) of
                buckets ->
                    {ok, Bs} = metric:list(),
                    Transport:send(Socket, dproto_tcp:encode_metrics(Bs)),
                    loop(Socket, Transport, State, Loop - 1);
                {list, Bucket} ->
                    {ok, Ms} = metric:list(Bucket),
                    Transport:send(Socket, dproto_tcp:encode_metrics(Ms)),
                    loop(Socket, Transport, State, Loop - 1);
                {get, B, M, T, C} ->
                    {ok, Resolution, Points} = metric:get(B, M, T, C),
                    Transport:send(Socket, <<Resolution:64/integer, Points/binary>>),
                    loop(Socket, Transport, State, Loop - 1);
                {stream, Bucket, Delay} ->
                    lager:info("[tcp] Entering stream mode for bucket '~s' "
                               "and a max delay of: ~p", [Bucket, Delay]),
                    ok = Transport:setopts(Socket, [{packet, 0}]),
                    {ok, CBin} = riak_core_ring_manager:get_chash_bin(),
                    N = State#state.n,
                    Nodes1 = chash:nodes(chashbin:to_chash(CBin)),
                    Nodes2 = [{I, riak_core_apl:get_apl(I, N, metric)}
                              || {I, _} <- Nodes1],
                    stream_loop(Socket, Transport,
                                #sstate{
                                   cbin = CBin,
                                   nodes = Nodes2,
                                   n = N,
                                   w = State#state.w,
                                   max_diff = Delay,
                                   bucket = Bucket},
                                dict:new(), {incomplete, <<>>})
            end;
        {error, timeout} ->
            loop(Socket, Transport, State, Loop - 1);
        {error, closed} ->
            ok;
        E ->
            lager:error("[tcp:loop] Error: ~p~n", [E]),
            ok = Transport:close(Socket)
    end.


stream_loop(Socket, Transport,
            State = #sstate{last = _L, max_diff = _Max, nodes = Nodes, w = W},
            Dict, {flush, Rest}) ->
    metric:mput(Nodes, Dict, W),
    {ok, CBin} = riak_core_ring_manager:get_chash_bin(),
    Nodes1 = chash:nodes(chashbin:to_chash(CBin)),
    Nodes2 = [{I, riak_core_apl:get_apl(I, State#sstate.n, metric)}
              || {I, _} <- Nodes1],
    State1 = State#sstate{nodes = Nodes2, cbin = CBin},
    stream_loop(Socket, Transport, State1, dict:new(),
                dproto_tcp:decode_stream(Rest));

stream_loop(Socket, Transport,
            State = #sstate{last = _L, max_diff = _Max, nodes = Nodes, w = W},
            Dict, {{stream, Metric, Time, Points}, Rest})
  when Time - _L > _Max ->
    metric:mput(Nodes, Dict, W),
    {ok, CBin} = riak_core_ring_manager:get_chash_bin(),
    Nodes1 = chash:nodes(chashbin:to_chash(CBin)),
    Nodes2 = [{I, riak_core_apl:get_apl(I, State#sstate.n, metric)}
              || {I, _} <- Nodes1],
    State1 = State#sstate{nodes = Nodes2, cbin = CBin, last=Time},
    Dict1 = insert_metric(State, dict:new(), Metric, Time, Points),
    stream_loop(Socket, Transport, State1, Dict1,
                dproto_tcp:decode_stream(Rest));

stream_loop(Socket, Transport, State, Dict,
            {{stream, Metric, Time, Points}, Rest}) ->
    Dict1 = insert_metric(State, Dict, Metric, Time, Points),
    stream_loop(Socket, Transport, State, Dict1,
                dproto_tcp:decode_stream(Rest));


stream_loop(Socket, Transport, State, Dict, {incomplete, Acc}) ->
    case Transport:recv(Socket, 0, 5000) of
        {ok, Data} ->
            Acc1 = <<Acc/binary, Data/binary>>,
            stream_loop(Socket, Transport, State, Dict,
                       dproto_tcp:decode_stream(Acc1));
        {error, timeout} ->
            stream_loop(Socket, Transport, State, Dict, {incomplete, Acc});
        {error,closed} ->
            metric:mput(State#sstate.nodes, Dict, State#sstate.w),
            ok;
        E ->
            lager:error("[tcp:stream] Error: ~p~n", [E]),
            metric:mput(State#sstate.nodes, Dict, State#sstate.w),
            ok = Transport:close(Socket)
    end.

insert_metric(#sstate{bucket = Bucket, cbin = CBin},
              Dict, Metric, Time, Points) ->
    DocIdx = riak_core_util:chash_key({Bucket, Metric}),
    {Idx, _} = chashbin:itr_value(chashbin:exact_iterator(DocIdx, CBin)),
    dict:append(Idx, {Bucket, Metric, Time, Points}, Dict).

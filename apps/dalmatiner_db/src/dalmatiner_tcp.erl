-module(dalmatiner_tcp).

-behaviour(ranch_protocol).

-include_lib("dproto/include/dproto.hrl").

-export([start_link/4]).
-export([init/4]).

-record(state, {cbin, nodes, n, w, fast_loop_count, wait = 5000}).

-record(sstate,
        {cbin, nodes, n, w, last = 0, max_diff = 1, wait = 5000,
         dict = dict:new(), bucket}).

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
        %% Simple keepalive
        {ok, <<?BUCKETS>>} ->
            {ok, Bs} = metric:list(),
            Transport:send(Socket, dproto_tcp:encode_metrics(Bs)),
            loop(Socket, Transport, State, Loop - 1);
        {ok, <<?LIST, L/binary>>} ->
            Bucket = dproto_tcp:decode_list(L),
            {ok, Ms} = metric:list(Bucket),
            Transport:send(Socket, dproto_tcp:encode_metrics(Ms)),
            loop(Socket, Transport, State, Loop - 1);
        {ok, <<?GET, G/binary>>} ->
            {B, M, T, C} = dproto_tcp:decode_get(G),
            {ok, Resolution, Data} = metric:get(B, M, T, C),
            Transport:send(Socket, <<Resolution:64/integer, Data/binary>>),
            loop(Socket, Transport, State, Loop - 1);
        {ok, <<?PUT,_BS:?BUCKET_SS/integer, Bucket:_BS/binary, D/binary>>} ->
            #state{cbin=CBin, nodes=Nodes, w=W} = State,
            case dalmatiner_db_udp:handle_data(D, Bucket, W, 0, CBin, Nodes, 0, dict:new()) of
                ok ->
                    loop(Socket, Transport, State, Loop - 1);
                E ->
                    lager:error("[tcp] Fast loop cancled because of: ~p.", [E]),
                    loop(Socket, Transport, State, Loop - 1)
            end;
        {ok, <<?STREAM, D:8, Bucket/binary>>} ->
            lager:info("[tcp] Entering stream mode for bucket '~s' "
                       "and a max delay of: ~p", [Bucket, D]),
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
                           max_diff = D,
                           bucket = Bucket},
                       dict:new(), <<>>);
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
            Dict, <<?SWRITE, Rest/binary>>) ->
    metric:mput(Nodes, Dict, W),
    {ok, CBin} = riak_core_ring_manager:get_chash_bin(),
    Nodes1 = chash:nodes(chashbin:to_chash(CBin)),
    Nodes2 = [{I, riak_core_apl:get_apl(I, State#sstate.n, metric)}
              || {I, _} <- Nodes1],
    State1 = State#sstate{nodes = Nodes2, cbin = CBin},
    stream_loop(Socket, Transport, State1, dict:new(), Rest);

stream_loop(Socket, Transport,
            State = #sstate{last = _L, max_diff = _Max, nodes = Nodes, w = W},
            Dict,
            <<?SENTRY,
              Time:?TIME_SIZE/integer,
              _MS:?METRIC_SS/integer, Metric:_MS/binary,
              _DS:?DATA_SS/integer, Points:_DS/binary, Rest/binary>>)
  when Time - _L > _Max ->
    metric:mput(Nodes, Dict, W),
    {ok, CBin} = riak_core_ring_manager:get_chash_bin(),
    Nodes1 = chash:nodes(chashbin:to_chash(CBin)),
    Nodes2 = [{I, riak_core_apl:get_apl(I, State#sstate.n, metric)}
              || {I, _} <- Nodes1],
    State1 = State#sstate{nodes = Nodes2, cbin = CBin, last=Time},
    Dict1 = insert_metric(State, dict:new(), Metric, Time, Points),
    stream_loop(Socket, Transport, State1, Dict1, Rest);

stream_loop(Socket, Transport, State, Dict,
            <<?SENTRY,
              Time:?TIME_SIZE/integer,
              _MS:?METRIC_SS/integer, Metric:_MS/binary,
              _DS:?DATA_SS/integer, Points:_DS/binary, Rest/binary>>) ->
    Dict1 = insert_metric(State, Dict, Metric, Time, Points),
    stream_loop(Socket, Transport, State, Dict1, Rest);


stream_loop(Socket, Transport, State, Dict, Acc) ->
    case Transport:recv(Socket, 0, 5000) of
        {ok, Data} ->
            Acc1 = <<Acc/binary, Data/binary>>,
            stream_loop(Socket, Transport, State, Dict, Acc1);
        {error, timeout} ->
            stream_loop(Socket, Transport, State, Dict, Acc);
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

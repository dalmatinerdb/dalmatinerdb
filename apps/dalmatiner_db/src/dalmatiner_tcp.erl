-module(dalmatiner_tcp).

-behaviour(ranch_protocol).

-include_lib("dproto/include/dproto.hrl").

-export([start_link/4]).
-export([init/4]).

-record(state, {cbin, nodes, n, w, fast_loop_count, wait = 5000}).

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
            io:format("list: ~s~n", [Bucket]),
            {ok, Ms} = metric:list(Bucket),
            Transport:send(Socket, dproto_tcp:encode_metrics(Ms)),
            loop(Socket, Transport, State, Loop - 1);
        {ok, <<?GET, G/binary>>} ->
            io:format("get~n"),
            {B, M, T, C} = dproto_tcp:decode_get(G),
            io:format(">~s/~s@~p: ~p~n", [B, M, T, C]),
            {ok, Resolution, Data} = metric:get(B, M, T, C),
            Transport:send(Socket, <<Resolution:64/integer, Data/binary>>),
            loop(Socket, Transport, State, Loop - 1);
        {ok, <<0,_BS:?BUCKET_SS/integer, Bucket:_BS/binary, D/binary>>} ->
            #state{cbin=CBin, nodes=Nodes, w=W} = State,
            case dalmatiner_db_udp:handle_data(D, Bucket, W, 0, CBin, Nodes, 0, dict:new()) of
                ok ->
                    loop(Socket, Transport, State, Loop - 1);
                E ->
                    lager:error("[tcp] Fast loop cancled because of: ~p.", [E]),
                    loop(Socket, Transport, State, Loop - 1)
            end;
        {error, timeout} ->
            loop(Socket, Transport, State, Loop - 1);
        E ->
            io:format("E: ~p~n", [E]),
            ok = Transport:close(Socket)
    end.

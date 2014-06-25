-module(metric_tcp).

-behaviour(ranch_protocol).

-define(KEEPALIVE, 0).
-define(LIST, 1).
-define(GET, 2).


-export([start_link/4]).
-export([init/4]).

start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

init(Ref, Socket, Transport, _Opts = []) ->
    ok = ranch:accept_ack(Ref),
    loop(Socket, Transport).

loop(Socket, Transport) ->
    case Transport:recv(Socket, 0, 5000) of
        %% Simple keepalive
        {ok, <<?KEEPALIVE>>} ->
            io:format("keepalive~n"),
            loop(Socket, Transport);
        {ok, <<?LIST>>} ->
            io:format("list~n"),
            {ok, Ms} = metric:list(),
            Transport:send(Socket, metric_tcp_proto:encode_metrics(Ms)),
            loop(Socket, Transport);
        {ok, <<?GET, G/binary>>} ->
            io:format("get~n"),
            {M, T, C} = metric_tcp_proto:decode_get(G),
            io:format(">~s@~p: ~p~n", [M, T, C]),
            {ok, Data} = metric:get(M, T, C),
            Transport:send(Socket, Data),
            loop(Socket, Transport);
        {error,timeout} ->
            loop(Socket, Transport);
        E ->
            io:format("E: ~p~n", [E]),
            ok = Transport:close(Socket)
    end.

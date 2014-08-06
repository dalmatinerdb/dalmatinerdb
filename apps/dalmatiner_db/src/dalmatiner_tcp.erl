-module(dalmatiner_tcp).

-behaviour(ranch_protocol).

-include_lib("dproto/include/dproto.hrl").

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
        {ok, <<?BUCKETS>>} ->
            io:format("list~n", []),
            {ok, Bs} = metric:list(),
            Transport:send(Socket, dproto_tcp:encode_metrics(Bs)),
            loop(Socket, Transport);
        {ok, <<?LIST, L/binary>>} ->
            Bucket = dproto_tcp:decode_list(L),
            io:format("list: ~s~n", [Bucket]),
            {ok, Ms} = metric:list(Bucket),
            Transport:send(Socket, dproto_tcp:encode_metrics(Ms)),
            loop(Socket, Transport);
        {ok, <<?GET, G/binary>>} ->
            io:format("get~n"),
            {B, M, T, C} = dproto_tcp:decode_get(G),
            io:format(">~s/~s@~p: ~p~n", [B, M, T, C]),
            {ok, Data, Resolution} = metric:get(B, M, T, C),
            Transport:send(Socket, <<Resolution:64/integer, Data/binary>>),
            loop(Socket, Transport);
        {error,timeout} ->
            loop(Socket, Transport);
        E ->
            io:format("E: ~p~n", [E]),
            ok = Transport:close(Socket)
    end.

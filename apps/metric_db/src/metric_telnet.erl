-module(metric_telnet).

-behaviour(ranch_protocol).

-define(KEEPALIVE, 0).
-define(LIST, 1).
-define(GET, 2).
-define(QRY, $q).


-export([start_link/4]).
-export([init/4]).

start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

init(Ref, Socket, Transport, _Opts = []) ->
    ok = ranch:accept_ack(Ref),
    Transport:setopts(Socket, [{packet, line}]),
    loop(Socket, Transport).

loop(Socket, Transport) ->
    case Transport:recv(Socket, 0, 5000) of
        %% Simple keepalive
        {ok, <<"quit\r\n">>} ->
            Transport:send(Socket, <<"bye!\r\n">>),
            ok = Transport:close(Socket);
        {ok, <<"metrics\r\n">>} ->
            {ok, Ms} = metric:list(),
            Ms1 = string:join([binary_to_list(M) || M <- Ms], ", "),
            Ms2 = list_to_binary(Ms1),
            Transport:send(Socket, <<Ms2/binary, "\r\n">>),
            loop(Socket, Transport);
        {ok, <<"e ", D/binary>>} ->
            QT = do_query_prep,
            R = io_lib:format("~p\n\r", [QT]),
            Transport:send(Socket, list_to_binary(R)),
            loop(Socket, Transport);
        {ok, <<"explain ", D/binary>>} ->
            QT = do_query_prep,
            R = io_lib:format("~p\n\r", [QT]),
            Transport:send(Socket, list_to_binary(R)),
            loop(Socket, Transport);
        {ok, <<"q ", D/binary>>} ->
            QT = do_query_prep(D),
            Data = metric_qry_parser:execute(QT),
            Transport:send(Socket, <<Data/binary, "\n\r">>),
            loop(Socket, Transport);
        {ok, <<"query ", D/binary>>} ->
            QT = do_query_prep(D),
            Data = metric_qry_parser:execute(QT),
            Transport:send(Socket, <<Data/binary, "\n\r">>),
            loop(Socket, Transport);
        {ok, What} ->
            Transport:send(Socket, <<"I do not understand: ", What/binary>>),
            loop(Socket, Transport);

        {error,timeout} ->
            loop(Socket, Transport);
        E ->
            io:format("E: ~p~n", [E]),
            ok = Transport:close(Socket)
    end.

do_query_prep(D) ->
    S = byte_size(D) - 2,
    <<Q:S/binary, "\r\n">> = D,
    R1 = metric_qry_parser:parse(Q),
    {to_list, R1}.

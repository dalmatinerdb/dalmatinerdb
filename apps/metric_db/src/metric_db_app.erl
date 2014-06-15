-module(metric_db_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    Port = case application:get_env(metric_db, tcp_port) of
               {ok, P} ->
                   P;
               _ ->
                   5555
           end,
    Listeners = case application:get_env(metric_db, tcp_listeners) of
                    {ok, L} ->
                        L;
                    _ ->
                        100
                end,
    {ok, _} = ranch:start_listener(metric_tcp, Listeners,
                                   ranch_tcp, [{port, Port}],
                                   metric_tcp, []),
    TelnetPort = case application:get_env(metric_db, telnet_port) of
               {ok, TP} ->
                   TP;
               _ ->
                   5556
           end,
    TelnetListeners = case application:get_env(metric_db, telnet_listeners) of
                    {ok, TL} ->
                        TL;
                    _ ->
                        2
                end,
    {ok, _} = ranch:start_listener(metric_telnet, TelnetListeners,
                                   ranch_tcp, [{port, TelnetPort}],
                                   metric_telnet, []),

    metric_db_sup:start_link().

stop(_State) ->
    ok.

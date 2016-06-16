-module(dalmatiner_db_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->

    folsom_metrics:new_histogram(put, slide, 60),
    folsom_metrics:new_histogram(mput, slide, 60),
    folsom_metrics:new_histogram(get, slide, 60),
    folsom_metrics:new_histogram(list_buckets, slide, 60),
    folsom_metrics:new_histogram(list_metrics, slide, 60),
    spawn(fun delay_tcp_anouncement/0),
    dalmatiner_db_sup:start_link().

stop(_State) ->
    ok.

delay_tcp_anouncement() ->
    riak_core:wait_for_application(dalmatiner_db),
    Services = riak_core_node_watcher:services(),
    delay_tcp_anouncement(Services).

delay_tcp_anouncement([S | R]) ->
    riak_core:wait_for_service(S),
    delay_tcp_anouncement(R);
delay_tcp_anouncement([]) ->
    dalmatiner_metrics:start(),
    lager:info("[ddb] Enabling TCP listener."),
    Port = case application:get_env(dalmatiner_db, tcp_port) of
               {ok, P} ->
                   P;
               _ ->
                   5555
           end,
    Listeners = case application:get_env(dalmatiner_db, tcp_listeners) of
                    {ok, L} ->
                        L;
                    _ ->
                        100
                end,
    MaxConn = application:get_env(dalmatiner_db, tcp_max_connections, 1024),
    {ok, _} = ranch:start_listener(dalmatiner_tcp, Listeners,
                                   ranch_tcp,
                                   [{port, Port},
                                    {max_connections, MaxConn}],
                                   dalmatiner_tcp, []).

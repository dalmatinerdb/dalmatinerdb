-module(dalmatiner_db_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, wait_for_metadata/0]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    folsom_metrics:new_gauge(mps),
    folsom_metrics:new_gauge(port_count),
    folsom_metrics:new_gauge(process_count),
    folsom_metrics:new_gauge(tcp_connections),
    folsom_metrics:new_histogram(get, slide, 60),
    folsom_metrics:new_histogram(put, slide, 60),
    folsom_metrics:new_histogram(mput, slide, 60),

    folsom_metrics:new_histogram({event, put}, slide, 60),
    folsom_metrics:new_histogram({event, get}, slide, 60),

    folsom_metrics:new_histogram({mstore, read}, slide, 60),
    folsom_metrics:new_histogram({mstore, write}, slide, 60),

    folsom_metrics:new_histogram(list_buckets, slide, 60),
    folsom_metrics:new_histogram(list_metrics, slide, 60),
    spawn(fun delay_tcp_anouncement/0),
    dalmatiner_db_sup:start_link().

stop(_State) ->
    ok.

delay_tcp_anouncement() ->
    riak_core:wait_for_application(dalmatiner_db),
    Services = riak_core_node_watcher:services(),
    wait_for_metadata(),
    delay_tcp_anouncement(Services).

delay_tcp_anouncement([S | R]) ->
    riak_core:wait_for_service(S),
    delay_tcp_anouncement(R);

delay_tcp_anouncement([]) ->
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

%% Wait for the metadata manager to be started
wait_for_metadata() ->
    case whereis(riak_core_metadata_manager) of
        Pid when is_pid(Pid) ->
            ok;
        _ ->
            timer:sleep(500),
            wait_for_metadata()
    end.

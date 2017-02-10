%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 13 Jun 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(dalmatiner_metrics).

-behaviour(gen_server).
-include_lib("dproto/include/dproto.hrl").
-include_lib("mstore/include/mstore.hrl").


%% API
-export([start_link/0, inc/2, inc/1, start/0, statistics/0, stop/0]).

-ignore_xref([start_link/0, inc/1, start/0, stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(INTERVAL, 1000).
-define(BUCKET, <<"dalmatinerdb">>).
-define(COUNTERS_MPS, ddb_counters_mps).

-record(state, {running = false, dict, prefix}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

start() ->
    gen_server:call(?SERVER, start).

stop() ->
    gen_server:call(?SERVER, stop).

inc(Type) ->
    inc(Type, 1).

inc(Type, N) when is_atom(Type) ->
    inc(atom_to_binary(Type, utf8), N);

inc(Type, N) when is_binary(Type) ->
    try
        ets:update_counter(?COUNTERS_MPS, {self(), Type}, N)
    catch
        error:badarg ->
            ets:insert(?COUNTERS_MPS, {{self(), Type}, N})
    end,
    ok.

statistics() ->
    Spec = folsom_metrics:get_metrics_info(),
    Prefix = [],
    Inbound = riak_core_handoff_manager:status({direction, inbound}),
    Outbound = riak_core_handoff_manager:status({direction, outbound}),
    Ms = [{Prefix ++ [<<"handoffs">>, <<"inbound">>], Inbound},
          {Prefix ++ [<<"handoffs">>, <<"outbound">>], Outbound}],
    do_metrics(Prefix, Spec, fun (KeyValue, Acc) -> [KeyValue | Acc] end, Ms).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    %% We want a high priority so we don't get scheduled back and have false
    %% reporting.
    process_flag(priority, high),
    ets:new(?COUNTERS_MPS,
            [named_table, set, public, {write_concurrency, true}]),
    {ok, N} = application:get_env(dalmatiner_db, n),
    {ok, W} = application:get_env(dalmatiner_db, w),
    lager:info("[metrics] Initializing metric watcher with N: ~p, W: ~p at an "
               "interval of ~pms.", [N, W, ?INTERVAL]),
    Dict = bkt_dict:new(?BUCKET, N, W),
    %% We will delay starting ticks until all services are started uo
    %% that way we won't try to send data before the services are ready
    case application:get_env(dalmatiner_db, self_monitor) of
        {ok, false} ->
            lager:info("[ddb] Self monitoring is disabled.");
        _ ->
            lager:info("[ddb] Self monitoring is enabled."),
            spawn(fun delay_tick/0)
    end,
    {ok, #state{dict = Dict, prefix = list_to_binary(atom_to_list(node()))}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(start, _From, State = #state{running = false}) ->
    erlang:send_after(?INTERVAL, self(), tick),
    Reply = ok,
    {reply, Reply, State#state{running = true}};
handle_call(stop, _From, State = #state{running = true}) ->
    Reply = ok,
    {reply, Reply, State#state{running = false}};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------


%% -record(fold_acc,
%%         {
%%           type,
%%           count,
%%           time,
%%           prefix,
%%           dict
%%         }).

%% write_fold_acc(#fold_acc{type = Type, count = Count,
%%                          dict = Dict, prefix = Prefix,
%%                          time = Time}) ->
%%     add_to_dict([Prefix, Type], Time, Count, Dict).

%% fold_count({Type, Count}, Acc = #fold_acc{type = Type, count = CountAcc}) ->
%%     Acc#fold_acc{count = CountAcc + Count};
%% fold_count({Type, Count}, Acc) ->
%%     Acc#fold_acc{dict = write_fold_acc(Acc), type = Type, count = Count}.

handle_info(tick,
            State = #state{running = true, prefix = _Prefix, dict = _Dict}) ->
    MPS = ets:tab2list(?COUNTERS_MPS),
    ets:delete_all_objects(?COUNTERS_MPS),
    P = lists:sum([Cnt || {_, Cnt} <- MPS]),
    folsom_metrics:notify({mps, P}),
    folsom_metrics:notify({port_count, erlang:system_info(port_count)}),
    folsom_metrics:notify({process_count, erlang:system_info(process_count)}),
    folsom_metrics:notify({tcp_connections,
                           ranch_server:count_connections(dalmatiner_tcp)}),

    %% TODO: prepare confuration switch to enable/disable saving internal
    %%       metrics
    %% Dict1 = bkt_dict:update_chash(Dict),
    %% Time = timestamp(),
    %% Spec = folsom_metrics:get_metrics_info(),

    %% Dict2 = case ets:tab2list(?COUNTERS_MPS) of
    %%             [] ->
    %%                 Dict1;
    %%             Counts ->
    %%                 ets:delete_all_objects(?COUNTERS_MPS),
    %%                 Counts1 = [{Type, Cnt} || {{_, Type}, Cnt} <- Counts],
    %%                 [{Type0, Count0} | Counts2] = lists:sort(Counts1),
    %%                 AccIn = #fold_acc{type = Type0, count = Count0, dict= Dict1,
    %%                                   time = Time, prefix = Prefix},
    %%                 AccOut = lists:foldl(fun fold_count/2, AccIn, Counts2),
    %%                 write_fold_acc(AccOut)
    %%         end,
    %% Dict3 = do_metrics(Prefix, Time, Spec, Dict2),
    %% Inbound = riak_core_handoff_manager:status({direction, inbound}),
    %% Outbound = riak_core_handoff_manager:status({direction, outbound}),

    %% Dict4 = add_to_dict([Prefix, <<"handoffs">>, <<"inbound">>],
    %%                     Time, length(Inbound), Dict3),
    %% Dict5 = add_to_dict([Prefix, <<"handoffs">>, <<"outbound">>],
    %%                     Time, length(Outbound), Dict4),
    %% %% Send
    %% Dict6 = bkt_dict:flush(Dict5),

    erlang:send_after(?INTERVAL, self(), tick),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_metrics(_Prefix, [], _Fun, Acc) ->
    Acc;

do_metrics(Prefix, [{N, [{type, histogram} | _]} | Spec], Fun, Acc) ->
    Stats = folsom_metrics:get_histogram_statistics(N),
    Prefix1 = [Prefix, metric_name(N)],
    Acc1 = build_histogram(Stats, Prefix1, Fun, Acc),
    do_metrics(Prefix, Spec, Fun, Acc1);

do_metrics(Prefix, [{N, [{type, spiral} | _]} | Spec], Fun, Acc) ->
    [{count, Count}, {one, One}] = folsom_metrics:get_metric_value(N),
    K = metric_name(N),
    Acc1 = add_metric(Prefix, {K, <<"count">>}, Count, Fun, Acc),
    Acc2 = add_metric(Prefix, {K, <<"one">>}, One, Fun, Acc1),
    do_metrics(Prefix, Spec, Fun, Acc2);

do_metrics(Prefix, [{N, [{type, counter} | _]} | Spec], Fun, Acc) ->
    Count = folsom_metrics:get_metric_value(N),
    Acc1 = add_metric(Prefix, N, Count, Fun, Acc),
    do_metrics(Prefix, Spec, Fun, Acc1);

do_metrics(Prefix, [{N, [{type, gauge} | _]} | Spec], Fun, Acc) ->
    Value = folsom_metrics:get_metric_value(N),
    Acc1 = add_metric(Prefix, N, Value, Fun, Acc),
    do_metrics(Prefix, Spec, Fun, Acc1);

do_metrics(Prefix, [{N, [{type, duration} | _]} | Spec], Fun, Acc) ->
    Stats = folsom_metrics:get_metric_value(N),
    Prefix1 = [Prefix, metric_name(N)],
    Acc1 = build_histogram(Stats, Prefix1, Fun, Acc),
    do_metrics(Prefix, Spec, Fun, Acc1);

do_metrics(Prefix, [{N, [{type, meter} | _]} | Spec], Fun, Acc) ->
    Prefix1 = [Prefix, metric_name(N)],
    [{count, Count},
     {one, One},
     {five, Five},
     {fifteen, Fifteen},
     {day, Day},
     {mean, Mean},
     {acceleration,
      [{one_to_five, OneToFive},
       {five_to_fifteen, FiveToFifteen},
       {one_to_fifteen, OneToFifteen}]}]
        = folsom_metrics:get_metric_value(N),
    Acc1 = add_metric(Prefix1, [<<"count">>], Count, Fun, Acc),
    Acc2 = add_metric(Prefix1, [<<"one">>], One, Fun, Acc1),
    Acc3 = add_metric(Prefix1, [<<"five">>], Five, Fun, Acc2),
    Acc4 = add_metric(Prefix1, [<<"fifteen">>], Fifteen, Fun, Acc3),
    Acc5 = add_metric(Prefix1, [<<"day">>], Day, Fun, Acc4),
    Acc6 = add_metric(Prefix1, [<<"mean">>], Mean, Fun, Acc5),
    Acc7 = add_metric(Prefix1, [<<"one_to_five">>], OneToFive, Fun, Acc6),
    Acc8 = add_metric(Prefix1,
                      [<<"five_to_fifteen">>], FiveToFifteen, Fun, Acc7),
    Acc9 = add_metric(Prefix1,
                      [<<"one_to_fifteen">>], OneToFifteen, Fun, Acc8),
    do_metrics(Prefix, Spec, Fun, Acc9).

add_metric(Prefix, Name, Value, Fun, Acc)  ->
    Fun({[Prefix, metric_name(Name)], Value}, Acc).

%% timestamp() ->
%%     os:system_time(seconds).

metric_name(B) when is_binary(B) ->
    B;
metric_name(L) when is_list(L) ->
    erlang:list_to_binary(L);
metric_name(N1) when
      is_atom(N1) ->
    a2b(N1);
metric_name({N1, N2}) when
      is_atom(N1), is_atom(N2) ->
    [a2b(N1), a2b(N2)];
metric_name({N1, N2, N3}) when
      is_atom(N1), is_atom(N2), is_atom(N3) ->
    [a2b(N1), a2b(N2), a2b(N3)];
metric_name({N1, N2, N3, N4}) when
      is_atom(N1), is_atom(N2), is_atom(N3), is_atom(N4) ->
    [a2b(N1), a2b(N2), a2b(N3), a2b(N4)];
metric_name(T) when is_tuple(T) ->
    [metric_name(E) || E <- tuple_to_list(T)].

a2b(A) ->
    erlang:atom_to_binary(A, utf8).

% build_histogram(Stats, Prefix, Time, Acc)
build_histogram([], _Prefix, _Fun, Acc) ->
    Acc;

build_histogram([{min, V} | H], Prefix, Fun, Acc) ->
    Acc1 = add_metric(Prefix, <<"min">>, V, Fun, Acc),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([{max, V} | H], Prefix, Fun, Acc) ->
    Acc1 = add_metric(Prefix, <<"max">>, V, Fun, Acc),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([{arithmetic_mean, V} | H], Prefix, Fun, Acc) ->
    Acc1 = add_metric(Prefix, <<"arithmetic_mean">>, V, Fun, Acc),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([{geometric_mean, V} | H], Prefix, Fun, Acc) ->
    Acc1 = add_metric(Prefix, <<"geometric_mean">>, V, Fun, Acc),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([{harmonic_mean, V} | H], Prefix, Fun, Acc) ->
    Acc1 = add_metric(Prefix, <<"harmonic_mean">>, V, Fun, Acc),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([{median, V} | H], Prefix, Fun, Acc) ->
    Acc1 = add_metric(Prefix, <<"median">>, V, Fun, Acc),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([{variance, V} | H], Prefix, Fun, Acc) ->
    Acc1 = add_metric(Prefix, <<"variance">>, V, Fun, Acc),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([{standard_deviation, V} | H], Prefix, Fun, Acc) ->
    Acc1 = add_metric(Prefix, <<"standard_deviation">>, V, Fun, Acc),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([{skewness, V} | H], Prefix, Fun, Acc) ->
    Acc1 = add_metric(Prefix, <<"skewness">>, V, Fun, Acc),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([{kurtosis, V} | H], Prefix, Fun, Acc) ->
    Acc1 = add_metric(Prefix, <<"kurtosis">>, V, Fun, Acc),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([{percentile, Ps} | H], Prefix, Fun, Acc) ->
    Acc1 = lists:foldl(
             fun({N, V}, AccIn) ->
                     P = integer_to_binary(N),
                     add_metric(Prefix, <<"p", P/binary>>, V, Fun, AccIn)
             end, Acc, Ps),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([{n, V} | H], Prefix, Fun, Acc) ->
    Acc1 = add_metric(Prefix, <<"count">>, V, Fun, Acc),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([_ | H], Prefix, Fun, Acc) ->
    build_histogram(H, Prefix, Fun, Acc).

delay_tick() ->
    riak_core:wait_for_application(dalmatiner_db),
    Services = riak_core_node_watcher:services(),
    dalmatiner_db_app:wait_for_metadata(),
    delay_tick(Services).

delay_tick([S | R]) ->
    riak_core:wait_for_service(S),
    delay_tick(R);

delay_tick([]) ->
    dalmatiner_metrics:start().

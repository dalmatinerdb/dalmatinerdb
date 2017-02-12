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
-export([start_link/0, start/0, stop/0]).

-ignore_xref([start_link/0, start/0, stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(INTERVAL, 1000).
-define(BUCKET, <<"dalmatinerdb">>).

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


handle_info(tick,
            State = #state{running = true, prefix = Prefix, dict = Dict0}) ->
    %% Initialize what we need
    Dict = bkt_dict:update_chash(Dict0),
    Time = timestamp(),

    %% Add our own counters
    Counts = ddb_counter:get_and_clean(),
    DictC = lists:foldl(fun ({Name, Count}, Acc) ->
                                add_to_dict([Prefix, Name], Time, Count, Acc)
                        end, Dict, Counts),

    %% Add our own histograms
    Hists = ddb_histogram:get(),
    DictH = lists:foldl(
              fun ({Name, Vals}, Acc1) ->
                      lists:foldl(
                        fun({K, V}, Acc2) ->
                                add_to_dict([Prefix, Name, K], Time, V, Acc2)
                        end, Acc1, Vals)
              end, DictC, Hists),

    %% Add folsom data to the dict
    Spec = folsom_metrics:get_metrics_info(),
    DictF = do_metrics(Prefix, Time, Spec, DictH),

    %% Add riak_core related data
    DictR = get_handoff_metrics(Prefix, Time, DictF),

    %% Send
    DictFlushed = bkt_dict:flush(DictR),
    erlang:send_after(?INTERVAL, self(), tick),
    {noreply, State#state{dict = DictFlushed}};

handle_info(_Info, State) ->
    {noreply, State}.

get_handoff_metrics(Prefix, Time, Dict) ->
    Inbound = riak_core_handoff_manager:status({direction, inbound}),
    Outbound = riak_core_handoff_manager:status({direction, outbound}),

    Dict1 = add_to_dict([Prefix, <<"handoffs">>, <<"inbound">>],
                        Time, length(Inbound), Dict),
    add_to_dict([Prefix, <<"handoffs">>, <<"outbound">>],
                Time, length(Outbound), Dict1).

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

%do_metrics(CBin, Time, Specs, Acc) ->
do_metrics(_Prefix, _Time, [], Acc) ->
    Acc;

do_metrics(Prefix, Time, [{N, [{type, histogram} | _]} | Spec], Acc) ->
    Stats = folsom_metrics:get_histogram_statistics(N),
    Prefix1 = [Prefix, metric_name(N)],
    Acc1 = build_histogram(Stats, Prefix1, Time, Acc),
    do_metrics(Prefix, Time, Spec, Acc1);

do_metrics(Prefix, Time, [{N, [{type, spiral} | _]} | Spec], Acc) ->
    [{count, Count}, {one, One}] = folsom_metrics:get_metric_value(N),
    K = metric_name(N),
    Acc1 = add_metric(Prefix, [K, <<"count">>], Time, Count, Acc),
    Acc2 = add_metric(Prefix, [K, <<"one">>], Time, One, Acc1),
    do_metrics(Prefix, Time, Spec, Acc2);


do_metrics(Prefix, Time, [{N, [{type, counter} | _]} | Spec], Acc) ->
    Count = folsom_metrics:get_metric_value(N),
    Acc1 = add_metric(Prefix, N, Time, Count, Acc),
    do_metrics(Prefix, Time, Spec, Acc1);

do_metrics(Prefix, Time, [{N, [{type, duration} | _]} | Spec], Acc) ->
    Stats = folsom_metrics:get_metric_value(N),
    Prefix1 = [Prefix, metric_name(N)],
    Acc1 = build_histogram(Stats, Prefix1, Time, Acc),
    do_metrics(Prefix, Time, Spec, Acc1);


do_metrics(Prefix, Time, [{N, [{type, meter} | _]} | Spec], Acc) ->
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
    Acc1 = add_metric(Prefix1, [<<"count">>], Time, Count, Acc),
    Acc2 = add_metric(Prefix1, [<<"one">>], Time, One, Acc1),
    Acc3 = add_metric(Prefix1, [<<"five">>], Time, Five, Acc2),
    Acc4 = add_metric(Prefix1, [<<"fifteen">>], Time, Fifteen, Acc3),
    Acc5 = add_metric(Prefix1, [<<"day">>], Time, Day, Acc4),
    Acc6 = add_metric(Prefix1, [<<"mean">>], Time, Mean, Acc5),
    Acc7 = add_metric(Prefix1, [<<"one_to_five">>], Time, OneToFive, Acc6),
    Acc8 = add_metric(Prefix1,
                      [<<"five_to_fifteen">>], Time, FiveToFifteen, Acc7),
    Acc9 = add_metric(Prefix1,
                      [<<"one_to_fifteen">>], Time, OneToFifteen, Acc8),
    do_metrics(Prefix, Time, Spec, Acc9).

add_metric(Prefix, Name, Time, Value, Acc) when is_integer(Value) ->
    add_to_dict([Prefix, metric_name(Name)], Time, Value, Acc);

add_metric(Prefix, Name, Time, Value, Acc) when is_float(Value) ->
    add_to_dict([Prefix, metric_name(Name)], Time, Value, Acc).

-spec add_to_dict([binary() | [binary()]], integer(), integer() | float(),
                  bkt_dict:bkt_dict()) ->
                         bkt_dict:bkt_dict().
add_to_dict(MetricL, Time, Value, Dict) ->
    Metric = dproto:metric_from_list(lists:flatten(MetricL)),
    Data = mmath_bin:from_list([Value]),
    bkt_dict:add(Metric, Time, Data, Dict).

timestamp() ->
    os:system_time(seconds).

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
build_histogram([], _Prefix, _Time, Acc) ->
    Acc;

build_histogram([{min, V} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"min">>, Time, V, Acc),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([{max, V} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"max">>, Time, V, Acc),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([{arithmetic_mean, V} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"arithmetic_mean">>, Time, V, Acc),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([{geometric_mean, V} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"geometric_mean">>, Time, V, Acc),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([{harmonic_mean, V} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"harmonic_mean">>, Time, V, Acc),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([{median, V} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"median">>, Time, V, Acc),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([{variance, V} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"variance">>, Time, V, Acc),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([{standard_deviation, V} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"standard_deviation">>, Time, V, Acc),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([{skewness, V} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"skewness">>, Time, V, Acc),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([{kurtosis, V} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"kurtosis">>, Time, V, Acc),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([{percentile, Ps} | H], Prefix, Time, Acc) ->
    Acc1 = lists:foldl(
             fun({N, V}, AccIn) ->
                     P = integer_to_binary(N),
                     add_metric(Prefix, <<"p", P/binary>>, Time, V, AccIn)
             end, Acc, Ps),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([{n, V} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"count">>, Time, V, Acc),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([_ | H], Prefix, Time, Acc) ->
    build_histogram(H, Prefix, Time, Acc).

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

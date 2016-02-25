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
-export([start_link/0, inc/1, inc/0, statistics/0]).

-ignore_xref([start_link/0, inc/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(INTERVAL, 1000).
-define(BUCKET, <<"dalmatinerdb">>).
-define(COUNTERS_MPS, ddb_counters_mps).


-record(state, {dict, prefix}).

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


inc() ->
    inc(1).

inc(N) ->
    try
        ets:update_counter(?COUNTERS_MPS, self(), N)
    catch
        error:badarg ->
            ets:insert(?COUNTERS_MPS, {self(), N})
    end,
    ok.

statistics() ->
    Spec = folsom_metrics:get_metrics_info(),
    do_metrics([], Spec, fun (KeyValue, Acc) -> [KeyValue | Acc] end, []).

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
    erlang:send_after(?INTERVAL, self(), tick),
    lager:info("[metrics] Initializing metric watcher with N: ~p, W: ~p at an "
               "interval of ~pms.", [N, W, ?INTERVAL]),
    Dict = bkt_dict:new(?BUCKET, N, W),
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

handle_info(tick, State = #state{prefix = _Prefix, dict = _Dict}) ->
    MPS = ets:tab2list(?COUNTERS_MPS),
    ets:delete_all_objects(?COUNTERS_MPS),
    P = lists:sum([Cnt || {_, Cnt} <- MPS]),
    folsom_metrics:notify({mps, P}),

    %% TODO: prepare confuration switch to enable/disable saving internal
    %%       metrics
    %% Dict1 = bkt_dict:update_chash(Dict),
    %% Time = timestamp(),
    %% Spec = folsom_metrics:get_metrics_info(),

    %% Dict2 = do_metrics(Prefix, Spec,
    %%                    fun ({Metric, Value}, Acc) ->
    %%                        add_to_dict(Metric, Time, Value, Acc)
    %%                    end, Dict1),
    %% Dict3 = bkt_dict:flush(Dict2),

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

add_metric(Prefix, Name, Value, Fun, Acc) when is_integer(Value) ->
    Fun({[Prefix, metric_name(Name)], Value}, Acc);

add_metric(Prefix, Name, Value, Fun, Acc) when is_float(Value) ->
    Scale = 1000*1000,
    add_metric(Prefix, Name, round(Value * Scale), Fun, Acc).

%% add_to_dict(Metric, Time, Value, Dict) ->
%%     Metric1 = dproto:metric_from_list(lists:flatten(Metric)),
%%     bkt_dict:add(Metric1, Time, mmath_bin:from_list([Value]), Dict).

%% timestamp() ->
%%     {Meg, S, _} = os:timestamp(),
%%     Meg*1000000 + S.

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
    Acc1 = add_metric(Prefix, <<"min">>, round(V), Fun, Acc),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([{max, V} | H], Prefix, Fun, Acc) ->
    Acc1 = add_metric(Prefix, <<"max">>, round(V), Fun, Acc),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([{arithmetic_mean, V} | H], Prefix, Fun, Acc) ->
    Acc1 = add_metric(Prefix, <<"arithmetic_mean">>, round(V), Fun, Acc),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([{geometric_mean, V} | H], Prefix, Fun, Acc) ->
    Acc1 = add_metric(Prefix, <<"geometric_mean">>, round(V), Fun, Acc),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([{harmonic_mean, V} | H], Prefix, Fun, Acc) ->
    Acc1 = add_metric(Prefix, <<"harmonic_mean">>, round(V), Fun, Acc),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([{median, V} | H], Prefix, Fun, Acc) ->
    Acc1 = add_metric(Prefix, <<"median">>, round(V), Fun, Acc),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([{variance, V} | H], Prefix, Fun, Acc) ->
    Acc1 = add_metric(Prefix, <<"variance">>, round(V), Fun, Acc),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([{standard_deviation, V} | H], Prefix, Fun, Acc) ->
    Acc1 = add_metric(Prefix, <<"standard_deviation">>, round(V), Fun, Acc),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([{skewness, V} | H], Prefix, Fun, Acc) ->
    Acc1 = add_metric(Prefix, <<"skewness">>, round(V), Fun, Acc),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([{kurtosis, V} | H], Prefix, Fun, Acc) ->
    Acc1 = add_metric(Prefix, <<"kurtosis">>, round(V), Fun, Acc),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([{percentile,
                  [{50, P50}, {75, P75}, {95, P95}, {99, P99}, {999, P999}]
                 } | H], Prefix, Fun, Acc) ->
    Acc1 = add_metric(Prefix, <<"p50">>, round(P50), Fun, Acc),
    Acc2 = add_metric(Prefix, <<"p75">>, round(P75), Fun, Acc1),
    Acc3 = add_metric(Prefix, <<"p95">>, round(P95), Fun, Acc2),
    Acc4 = add_metric(Prefix, <<"p99">>, round(P99), Fun, Acc3),
    Acc5 = add_metric(Prefix, <<"p999">>, round(P999), Fun, Acc4),
    build_histogram(H, Prefix, Fun, Acc5);

build_histogram([{n, V} | H], Prefix, Fun, Acc) ->
    Acc1 = add_metric(Prefix, <<"count">>, V, Fun, Acc),
    build_histogram(H, Prefix, Fun, Acc1);

build_histogram([_ | H], Prefix, Fun, Acc) ->
    build_histogram(H, Prefix, Fun, Acc).

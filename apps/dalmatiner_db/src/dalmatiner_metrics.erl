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
-export([start_link/0, inc/1, inc/0]).

-ignore_xref([start_link/0, inc/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(INTERVAL, 1000).
-define(BUCKET, <<"dalmatinerdb">>).
-define(COUNTERS_MPS, ddb_counters_mps).


-record(state, {n, w, prefix, ppf}).

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
    ets:new(?COUNTERS_MPS, [named_table, set, public, {write_concurrency, true}]),
    {ok, N} = application:get_env(dalmatiner_db, n),
    {ok, W} = application:get_env(dalmatiner_db, w),
    erlang:send_after(?INTERVAL, self(), tick),
    lager:info("[metrics] Initializing metric watcher with N: ~p, W: ~p at an "
               "interval of ~pms.", [N, W, ?INTERVAL]),
    PPF = metric:ppf(?BUCKET),
    {ok, #state{n=N, w=W, prefix = list_to_binary(atom_to_list(node())),
                ppf = PPF}}.


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

handle_info(tick, State = #state{prefix = Prefix, n = N, w = W, ppf = PPF}) ->
    {ok, CBin} = riak_core_ring_manager:get_chash_bin(),
    Nodes = chash:nodes(chashbin:to_chash(CBin)),
    Nodes1 = [{I, riak_core_apl:get_apl(I, N, metric)} || {I, _} <- Nodes],
    Time = timestamp(),
    Spec = folsom_metrics:get_metrics_info(),

    Dict = dict:new(),

    MPS = ets:tab2list(?COUNTERS_MPS),
    ets:delete_all_objects(?COUNTERS_MPS),
    P = lists:sum([Cnt || {_, Cnt} <- MPS]),

    Dict1 = add_to_dict(CBin, [Prefix, <<"mps">>], PPF, Time, P, Dict),
    Metrics = do_metrics(Prefix, CBin, PPF, Time, Spec, Dict1),
    metric:mput(Nodes1, Metrics, W),

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

%do_metrics(CBin, PPF, Time, Specs, Acc) ->
do_metrics(_Prefix, _CBin, _PPF, _Time, [], Acc) ->
    Acc;

do_metrics(Prefix, CBin, PPF, Time, [{N, [{type, histogram}]} | Spec], Acc) ->
    Stats = folsom_metrics:get_histogram_statistics(N),
    Prefix1 = [Prefix, metric_name(N)],
    Acc1 = build_histogram(Stats, Prefix1, PPF, Time, CBin, Acc),
    do_metrics(Prefix, CBin, PPF, Time, Spec, Acc1);

do_metrics(Prefix, CBin, PPF, Time, [{N, [{type, spiral}]} | Spec], Acc) ->
    [{count, Count}, {one, One}] = folsom_metrics:get_metric_value(N),
    K = metric_name(N),
    Acc1 = add_metric(Prefix, CBin, [K, <<"count">>], PPF, Time, Count, Acc),
    Acc2 = add_metric(Prefix, CBin, [K, <<"one">>], PPF, Time, One, Acc1),
    do_metrics(Prefix, CBin, PPF, Time, Spec, Acc2);


do_metrics(Prefix, CBin, PPF, Time, [{N, [{type, counter}]} | Spec], Acc) ->
    Count = folsom_metrics:get_metric_value(N),
    Acc1 = add_metric(Prefix, CBin, N, PPF, Time, Count, Acc),
    do_metrics(Prefix, CBin, PPF, Time, Spec, Acc1);

do_metrics(Prefix, CBin, PPF, Time, [{N, [{type, duration}]} | Spec], Acc) ->
    Stats = folsom_metrics:get_metric_value(N),
    Prefix1 = [Prefix, metric_name(N)],
    Acc1 = build_histogram(Stats, Prefix1, PPF, Time, CBin, Acc),
    do_metrics(Prefix, CBin, PPF, Time, Spec, Acc1);


do_metrics(Prefix, CBin, PPF, Time, [{N, [{type, meter}]} | Spec], Acc) ->
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
    Acc1 = add_metric(Prefix1, CBin, [<<"count">>], PPF, Time, Count, Acc),
    Acc2 = add_metric(Prefix1, CBin, [<<"one">>], PPF, Time, One, Acc1),
    Acc3 = add_metric(Prefix1, CBin, [<<"five">>], PPF, Time, Five, Acc2),
    Acc4 = add_metric(Prefix1, CBin, [<<"fifteen">>], PPF, Time, Fifteen, Acc3),
    Acc5 = add_metric(Prefix1, CBin, [<<"day">>], PPF, Time, Day, Acc4),
    Acc6 = add_metric(Prefix1, CBin, [<<"mean">>], PPF, Time, Mean, Acc5),
    Acc7 = add_metric(Prefix1, CBin, [<<"one_to_five">>], PPF, Time, OneToFive, Acc6),
    Acc8 = add_metric(Prefix1, CBin, [<<"five_to_fifteen">>], PPF, Time, FiveToFifteen, Acc7),
    Acc9 = add_metric(Prefix1, CBin, [<<"one_to_fifteen">>], PPF, Time, OneToFifteen, Acc8),
    do_metrics(Prefix, CBin, PPF, Time, Spec, Acc9).

add_metric(Prefix, CBin, Name, PPF, Time, Value, Acc) when is_integer(Value) ->
    add_to_dict(CBin, [Prefix, metric_name(Name)], PPF, Time, Value, Acc);

add_metric(Prefix, CBin, Name, PPF, Time, Value, Acc) when is_float(Value) ->
    Scale = 1000*1000,
    add_to_dict(CBin, [Prefix, metric_name(Name)], PPF, Time, round(Value*Scale), Acc).

add_to_dict(CBin, Metric, PPF, Time, Value, Acc) ->
    Metric1 = dproto:metric_from_list(lists:flatten(Metric)),
    DocIdx = riak_core_util:chash_key({?BUCKET, {Metric, Time div PPF}}),
    {Idx, _} = chashbin:itr_value(chashbin:exact_iterator(DocIdx, CBin)),
    dict:append(Idx, {?BUCKET, Metric1, Time, mmath_bin:from_list([Value])}, Acc).

timestamp() ->
    {Meg, S, _} = os:timestamp(),
    Meg*1000000 + S.

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

% build_histogram(Stats, Prefix, PPF, Time, CBin, Acc)
build_histogram([], _, _, _, _, Acc) ->
    Acc;

build_histogram([{min, V} | H], Prefix, PPF, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"min">>, PPF, Time, round(V), Acc),
    build_histogram(H, Prefix, PPF, Time, CBin, Acc1);

build_histogram([{max, V} | H], Prefix, PPF, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"max">>, PPF, Time, round(V), Acc),
    build_histogram(H, Prefix, PPF, Time, CBin, Acc1);

build_histogram([{arithmetic_mean, V} | H], Prefix, PPF, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"arithmetic_mean">>, PPF, Time, round(V), Acc),
    build_histogram(H, Prefix, PPF, Time, CBin, Acc1);

build_histogram([{geometric_mean, V} | H], Prefix, PPF, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"geometric_mean">>, PPF, Time, round(V), Acc),
    build_histogram(H, Prefix, PPF, Time, CBin, Acc1);

build_histogram([{harmonic_mean, V} | H], Prefix, PPF, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"harmonic_mean">>, PPF, Time, round(V), Acc),
    build_histogram(H, Prefix, PPF, Time, CBin, Acc1);

build_histogram([{median, V} | H], Prefix, PPF, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"median">>, PPF, Time, round(V), Acc),
    build_histogram(H, Prefix, PPF, Time, CBin, Acc1);

build_histogram([{variance, V} | H], Prefix, PPF, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"variance">>, PPF, Time, round(V), Acc),
    build_histogram(H, Prefix, PPF, Time, CBin, Acc1);

build_histogram([{standard_deviation, V} | H], Prefix, PPF, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"standard_deviation">>, PPF, Time, round(V), Acc),
    build_histogram(H, Prefix, PPF, Time, CBin, Acc1);

build_histogram([{skewness, V} | H], Prefix, PPF, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"skewness">>, PPF, Time, round(V), Acc),
    build_histogram(H, Prefix, PPF, Time, CBin, Acc1);

build_histogram([{kurtosis, V} | H], Prefix, PPF, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"kurtosis">>, PPF, Time, round(V), Acc),
    build_histogram(H, Prefix, PPF, Time, CBin, Acc1);

build_histogram([{percentile,
                  [{50, P50}, {75, P75}, {90, P90}, {95, P95}, {99, P99},
                   {999, P999}]} | H], Prefix, PPF, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"p50">>, PPF, Time, round(P50), Acc),
    Acc2 = add_metric(Prefix, CBin, <<"p75">>, PPF, Time, round(P75), Acc1),
    Acc3 = add_metric(Prefix, CBin, <<"p90">>, PPF, Time, round(P90), Acc2),
    Acc4 = add_metric(Prefix, CBin, <<"p95">>, PPF, Time, round(P95), Acc3),
    Acc5 = add_metric(Prefix, CBin, <<"p99">>, PPF, Time, round(P99), Acc4),
    Acc6 = add_metric(Prefix, CBin, <<"p999">>, PPF, Time, round(P999), Acc5),
    build_histogram(H, Prefix, PPF, Time, CBin, Acc6);

build_histogram([{n, V} | H], Prefix, PPF, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"count">>, PPF, Time, V, Acc),
    build_histogram(H, Prefix, PPF, Time, CBin, Acc1);

build_histogram([_ | H], Prefix, PPF, Time, CBin, Acc) ->
    build_histogram(H, Prefix, PPF, Time, CBin, Acc).

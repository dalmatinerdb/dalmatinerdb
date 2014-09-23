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
-export([start_link/0]).

-ignore_xref([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(INTERVAL, 1000).
-define(BUCKET, <<"dalmatinerdb">>).

-record(state, {n, w}).

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
    {ok, N} = application:get_env(dalmatiner_db, n),
    {ok, W} = application:get_env(dalmatiner_db, w),
    erlang:send_after(?INTERVAL, self(), tick),
    lager:info("[metrics] Initializing metric watcher with N: ~p, W: ~p at an "
               "interval of ~pms.", [N, W, ?INTERVAL]),
    {ok, #state{n=N, w=W}}.

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
handle_info(tick, State) ->
    {ok, CBin} = riak_core_ring_manager:get_chash_bin(),
    Nodes = chash:nodes(chashbin:to_chash(CBin)),
    Nodes1 = [{I, riak_core_apl:get_apl(I, State#state.n, metric)} || {I, _} <- Nodes],
    Time = timestamp(),
    Spec = folsom_metrics:get_metrics_info(),
    [Prefix|_] = re:split(atom_to_list(node()), "@"),
    Metrics = do_metrics(Prefix, CBin, Time, Spec, dict:new()),
    metric:mput(Nodes1, Metrics, State#state.w),
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

%do_metrics(CBin, Time, Specs, Acc) ->
do_metrics(_Prefix, _CBin, _Time, [], Acc) ->
    Acc;

do_metrics(Prefix, CBin, Time, [{N, [{type, histogram}]} | Spec], Acc) ->
    Stats = folsom_metrics:get_histogram_statistics(N),
    Prefix1 = <<Prefix/binary, ".", (metric_name(N))/binary>>,
    Acc1 = build_histogram(Stats, Prefix1, Time, CBin, Acc),
    do_metrics(Prefix, CBin, Time, Spec, Acc1);

do_metrics(Prefix, CBin, Time, [{N, [{type, spiral}]} | Spec], Acc) ->
    [{count, Count}, {one, One}] = folsom_metrics:get_metric_value(N),
    K = metric_name(N),
    Acc1 = add_metric(Prefix, CBin, <<K/binary, ".count">>, Time, Count, Acc),
    Acc2 = add_metric(Prefix, CBin, <<K/binary, ".one">>, Time, One, Acc1),
    do_metrics(Prefix, CBin, Time, Spec, Acc2);


do_metrics(Prefix, CBin, Time, [{N, [{type, counter}]} | Spec], Acc) ->
    Count = folsom_metrics:get_metric_value(N),
    Acc1 = add_metric(Prefix, CBin, N, Time, Count, Acc),
    do_metrics(Prefix, CBin, Time, Spec, Acc1);

do_metrics(Prefix, CBin, Time, [{N, [{type, duration}]} | Spec], Acc) ->
    Stats = folsom_metrics:get_metric_value(N),
    Prefix1 = <<Prefix/binary, ".", (metric_name(N))/binary>>,
    Acc1 = build_histogram(Stats, Prefix1, Time, CBin, Acc),
    do_metrics(Prefix, CBin, Time, Spec, Acc1);


do_metrics(Prefix, CBin, Time, [{N, [{type, meter}]} | Spec], Acc) ->
    Prefix1 = <<Prefix/binary, ".", (metric_name(N))/binary>>,
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
    Acc1 = add_metric(Prefix1, CBin, <<"count">>, Time, Count, Acc),
    Acc2 = add_metric(Prefix1, CBin, <<"one">>, Time, One, Acc1),
    Acc3 = add_metric(Prefix1, CBin, <<"five">>, Time, Five, Acc2),
    Acc4 = add_metric(Prefix1, CBin, <<"fifteen">>, Time, Fifteen, Acc3),
    Acc5 = add_metric(Prefix1, CBin, <<"day">>, Time, Day, Acc4),
    Acc6 = add_metric(Prefix1, CBin, <<"mean">>, Time, Mean, Acc5),
    Acc7 = add_metric(Prefix1, CBin, <<"one_to_five">>, Time, OneToFive, Acc6),
    Acc8 = add_metric(Prefix1, CBin, <<"five_to_fifteen">>, Time, FiveToFifteen, Acc7),
    Acc9 = add_metric(Prefix1, CBin, <<"one_to_fifteen">>, Time, OneToFifteen, Acc8),
    do_metrics(Prefix, CBin, Time, Spec, Acc9).

add_metric(Prefix, CBin, Name, Time, Value, Acc) when is_integer(Value) ->
    Metric = <<Prefix/binary, ".", (metric_name(Name))/binary>>,
    DocIdx = riak_core_util:chash_key({?BUCKET, Metric}),
    {Idx, _} = chashbin:itr_value(chashbin:exact_iterator(DocIdx, CBin)),
    dict:append(Idx, {?BUCKET, Metric, Time, <<1, Value:64/unsigned-integer>>}, Acc);

add_metric(Prefix, CBin, Name, Time, Value, Acc) when is_float(Value) ->
    Scale = 1000*1000,
    Metric = <<Prefix/binary, ".", (metric_name(Name))/binary>>,
    DocIdx = riak_core_util:chash_key({?BUCKET, Metric}),
    {Idx, _} = chashbin:itr_value(chashbin:exact_iterator(DocIdx, CBin)),
    dict:append(Idx, {?BUCKET, Metric, Time, <<1, (round(Value*Scale)):64/unsigned-integer>>}, Acc).

timestamp() ->
    {Meg, S, _} = os:timestamp(),
    Meg*1000000 + S.

metric_name(N1) when is_atom(N1) ->
    erlang:atom_to_binary(N1, utf8);
metric_name({N1, N2}) when is_atom(N1), is_atom(N2) ->
    <<(erlang:atom_to_binary(N1, utf8))/binary, ".",
      (erlang:atom_to_binary(N2, utf8))/binary>>;
metric_name({N1, N2, N3}) when is_atom(N1), is_atom(N2), is_atom(N3) ->
    <<(erlang:atom_to_binary(N1, utf8))/binary, ".",
      (erlang:atom_to_binary(N2, utf8))/binary, ".",
      (erlang:atom_to_binary(N3, utf8))/binary>>;
metric_name({N1, N2, N3}) when is_atom(N1), is_atom(N2), is_atom(N3) ->
    <<(erlang:atom_to_binary(N1, utf8))/binary, ".",
      (erlang:atom_to_binary(N2, utf8))/binary, ".",
      (erlang:atom_to_binary(N3, utf8))/binary>>;
metric_name({N1, N2, N3, N4}) when is_atom(N1), is_atom(N2), is_atom(N3), is_atom(N4) ->
    <<(erlang:atom_to_binary(N1, utf8))/binary, ".",
      (erlang:atom_to_binary(N2, utf8))/binary, ".",
      (erlang:atom_to_binary(N3, utf8))/binary, ".",
      (erlang:atom_to_binary(N4, utf8))/binary>>;
metric_name(A) when is_atom(A) ->
    erlang:atom_to_binary(A, utf8);
metric_name(B) when is_binary(B) ->
    B;
metric_name(L) when is_list(L) ->
    erlang:list_to_binary(L);
metric_name(T) when is_tuple(T) ->
    metric_name(tuple_to_list(T),<<>>).

metric_name([N | R], <<>>) ->
    metric_name(R, metric_name(N));

metric_name([N | R], Acc) ->
    metric_name(R, <<Acc/binary, ".", (metric_name(N))/binary>>);

metric_name([], Acc) ->
    Acc.

% build_histogram(Stats, Prefix, Time, CBin, Acc)
build_histogram([], _, _, _, Acc) ->
    Acc;

build_histogram([{min, V} | H], Prefix, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"min">>, Time, round(V), Acc),
    build_histogram(H, Prefix, Time, CBin, Acc1);

build_histogram([{max, V} | H], Prefix, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"max">>, Time, round(V), Acc),
    build_histogram(H, Prefix, Time, CBin, Acc1);

build_histogram([{arithmetic_mean, V} | H], Prefix, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"arithmetic_mean">>, Time, round(V), Acc),
    build_histogram(H, Prefix, Time, CBin, Acc1);

build_histogram([{geometric_mean, V} | H], Prefix, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"geometric_mean">>, Time, round(V), Acc),
    build_histogram(H, Prefix, Time, CBin, Acc1);

build_histogram([{harmonic_mean, V} | H], Prefix, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"harmonic_mean">>, Time, round(V), Acc),
    build_histogram(H, Prefix, Time, CBin, Acc1);

build_histogram([{median, V} | H], Prefix, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"median">>, Time, round(V), Acc),
    build_histogram(H, Prefix, Time, CBin, Acc1);

build_histogram([{variance, V} | H], Prefix, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"variance">>, Time, round(V), Acc),
    build_histogram(H, Prefix, Time, CBin, Acc1);

build_histogram([{standard_deviation, V} | H], Prefix, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"standard_deviation">>, Time, round(V), Acc),
    build_histogram(H, Prefix, Time, CBin, Acc1);

build_histogram([{skewness, V} | H], Prefix, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"skewness">>, Time, round(V), Acc),
    build_histogram(H, Prefix, Time, CBin, Acc1);

build_histogram([{kurtosis, V} | H], Prefix, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"kurtosis">>, Time, round(V), Acc),
    build_histogram(H, Prefix, Time, CBin, Acc1);

build_histogram([{percentile,
                  [{50, P50}, {75, P75}, {90, P90}, {95, P95}, {99, P99},
                   {999, P999}]} | H], Prefix, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<".p50">>, Time, round(P50), Acc),
    Acc2 = add_metric(Prefix, CBin, <<".p75">>, Time, round(P75), Acc1),
    Acc3 = add_metric(Prefix, CBin, <<".p90">>, Time, round(P90), Acc2),
    Acc4 = add_metric(Prefix, CBin, <<".p95">>, Time, round(P95), Acc3),
    Acc5 = add_metric(Prefix, CBin, <<".p99">>, Time, round(P99), Acc4),
    Acc6 = add_metric(Prefix, CBin, <<".p999">>, Time, round(P999), Acc5),
    build_histogram(H, Prefix, Time, CBin, Acc6);

build_histogram([{n, V} | H], Prefix, Time, CBin, Acc) ->
    Acc1 = add_metric(Prefix, CBin, <<"count">>, Time, V, Acc),
    build_histogram(H, Prefix, Time, CBin, Acc1);

build_histogram([_ | H], Prefix, Time, CBin, Acc) ->
    build_histogram(H, Prefix, Time, CBin, Acc).

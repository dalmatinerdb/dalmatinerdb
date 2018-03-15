%% @doc The coordinator for stat get operations.  The key here is to
%% generate the preflist just like in wrtie_fsm and then query each
%% replica and wait until a quorum is met.
-module(dalmatiner_read_fsm).
-behavior(gen_fsm).

-define(DEFAULT_TIMEOUT, 5000).

-include_lib("mmath/include/mmath.hrl").

%% API
-export([start_link/7, start/2, start/3, start/5]).


-export([reconcile/1, different/1, needs_repair/2, unique/1]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2, waiting/2, wait_for_n/2, finalize/2]).
-export_type([read_opts/0]).

-type partition() :: chash:index_as_int().
-type reply_src() :: {partition(), term()}.
-type metric_element() :: binary().
-type read_opts() :: [dproto_tcp:read_repair_opt() |
                      hpts |
                      {read_repair, {partial, pos_integer()}} |
                      dproto_tcp:read_r_opt() |
                      {n, pos_integer()}].
%%-type metric_reply() :: {partition(), metric_element()}.

-record(state, {req_id,
                read_repair = true :: boolean() | {partial, pos_integer()},
                compression :: snappy | none,
                from,
                entity,
                op,
                r,
                n,
                preflist,
                num_r = 0,
                overloaded = 0,
                size,
                timeout=?DEFAULT_TIMEOUT,
                val,
                vnode,
                system,
                hpts = false :: boolean(),
                replies=[]   :: [reply_src()]}).
-type state() :: #state{}.

-ignore_xref([
              code_change/4,
              different/1,
              execute/2,
              finalize/2,
              handle_event/3,
              handle_info/3,
              handle_sync_event/4,
              init/1,
              needs_repair/2,
              prepare/2,
              reconcile/1,
              repair/5,
              start/2,
              start/5,
              start_link/7,
              terminate/3,
              unique/1,
              wait_for_n/2,
              waiting/2,
              start/3
             ]).

%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqID, {VNode, System}, Op, From, Entity, Val, Opts) ->
    gen_fsm:start_link(
      ?MODULE, [ReqID, {VNode, System}, Op, From, Entity, Val, Opts], []).
start(VNodeInfo, Op) ->
    start(VNodeInfo, Op, undefined).

start(VNodeInfo, Op, Entity) ->
    start(VNodeInfo, Op, Entity, undefined, []).

-spec start({atom(), atom()}, atom(), atom(), term(), read_opts()) ->
                   ok | {ok, term()} | {error, timeout}.
start(VNodeInfo, Op, Entity, Val, Opts) ->
    ReqID = mk_reqid(),
    dalmatiner_read_fsm_sup:start_read_fsm(
      [ReqID, VNodeInfo, Op, self(), Entity, Val, Opts]),
    receive
        {ReqID, ok} ->
            ok;
        {ReqID, ok, Result} ->
            {ok, Result}
    after ?DEFAULT_TIMEOUT ->
            {error, timeout}
    end.

%%%===================================================================
%%% States
%%%===================================================================

%% Intiailize state data.
-spec init([any()]) ->
                  {ok, prepare, state(), 0}.

init([ReqId, {VNode, System}, Op, From]) ->
    init([ReqId, {VNode, System}, Op, From, undefined, undefined, []]);

init([ReqId, {VNode, System}, Op, From, Entity]) ->
    init([ReqId, {VNode, System}, Op, From, Entity, undefined, []]);

init([ReqId, {VNode, System}, Op, From, Entity, Val, Opts]) ->
    {ok, RR} = get_rr_opt(Opts),
    {ok, N} = get_n_opt(Opts),
    {ok, R} = get_r_opt(Opts, N),
    HPTS = proplists:get_bool(hpts, Opts),
    Compression = application:get_env(dalmatiner_db,
                                      metric_transport_compression, snappy),

    SD = #state{req_id = ReqId,
                compression = Compression,
                read_repair = RR,
                r = R,
                n = N,
                from = From,
                op = Op,
                val = Val,
                vnode = VNode,
                hpts = HPTS,
                system = System,
                entity = Entity},
    {ok, prepare, SD, 0}.

%% @doc Calculate the Preflist.
prepare(timeout, SD0=#state{entity={B, M},
                            system=System,
                            n=N}) ->

    DocIdx = riak_core_util:chash_key({B, M}),
    Prelist = riak_core_apl:get_apl(DocIdx, N, System),
    SD = SD0#state{preflist=Prelist},
    {next_state, execute, SD, 0}.

%% @doc Execute the get reqs.
execute(timeout, SD0=#state{req_id=ReqId,
                            entity=Entity,
                            op=Op,
                            val=Val,
                            hpts = HPTS,
                            preflist=Prelist}) ->
    case Entity of
        undefined ->
            metric_vnode:Op(Prelist, ReqId);
        {Bucket, {Metric, _}} ->
            case {Op, Val} of
                {_, undefined} ->
                    metric_vnode:Op(Prelist, ReqId, {Bucket, Metric});
                {get, Val} ->
                    metric_vnode:get(Prelist, ReqId, {Bucket, Metric},
                                     HPTS, Val);
                {_, Val} ->
                    metric_vnode:Op(Prelist, ReqId, {Bucket, Metric}, Val)
            end;
        {Bucket, _Time} ->
            case Val of
                undefined ->
                    event_vnode:Op(Prelist, ReqId, Bucket);
                _ ->
                    event_vnode:Op(Prelist, ReqId, Bucket, Val)
            end
    end,
    {next_state, waiting, SD0}.

%% @doc Wait for R replies and then respond to From (original client
%% that called `get/2').
%% `IdxNode' is a 2-tuple, {Partition, Node}, referring to the origin of this
%% reply
%% TODO: read repair...or another blog post?

is_done(#state{num_r = NumR, overloaded = O, n = N})
  when NumR + O == N ->
    finished;
is_done(#state{num_r = NumR, r = R, read_repair = false})
  when NumR >= R ->
    finished;
is_done(#state{num_r = NumR, r = R})
  when NumR >= R ->
    waiting;
is_done(_) ->
    false.

reply(SD = #state{from = From, replies = Replies, req_id=ReqID}) ->
    case merge(SD, Replies) of
        not_found ->
            From ! {ReqID, ok, not_found};
        Merged ->
            From ! {ReqID, ok, Merged}
    end.

check_waiting(SD = #state{timeout = Timeout}) ->
    case is_done(SD) of
        false ->
            {next_state, waiting, SD};
        finished ->
            reply(SD),
            {next_state, finalize, SD, 0};
        waiting ->
            reply(SD),
            {next_state, wait_for_n, SD, Timeout}
    end.

waiting({fail, _Idx, overload},
        SD = #state{overloaded = O}) ->
    check_waiting(SD#state{overloaded = O + 1});
waiting({fail, _ReqID, _Idx, overload},
        SD = #state{overloaded = O}) ->
    check_waiting(SD#state{overloaded = O + 1});

waiting({ok, _ReqID, IdxNode, Obj},
        SD0 = #state{num_r=NumR0}) ->
    SD1 = save_reply(IdxNode, Obj, SD0#state{num_r = NumR0 + 1}),
    check_waiting(SD1).

wait_for_n({fail, _ReqID, _Idx, overload}, SD) ->
    %% If we have a overload situation we simply won't repair
    %% this prevents escalating the overload
    {stop, normal, SD};

wait_for_n({ok, _ReqID, IdxNode, Obj},
           SD0=#state{n = N, num_r = NumR}) when NumR == N - 1 ->
    SD1 = save_reply(IdxNode, Obj, SD0),
    {next_state, finalize, SD1#state{num_r=N}, 0};

wait_for_n({ok, _ReqID, IdxNode, Obj},
           SD0=#state{num_r=NumR0, timeout=Timeout}) ->
    NumR = NumR0 + 1,
    SD1 = save_reply(IdxNode, Obj, SD0),
    {next_state, wait_for_n, SD1#state{num_r = NumR}, Timeout};

%% TODO partial repair?
wait_for_n(timeout, SD) ->
    lager:warning("Waiting for n failed: ~p", [SD]),
    {stop, normal, SD}.

finalize(_, SD=#state{read_repair = false}) ->
    {stop, normal, SD};


finalize(timeout, SD=#state{
                        val = {Time, _},
                        read_repair = RR,
                        replies=Replies,
                        hpts = HPTS,
                        entity=Entity = {Bucket, {_Metric, _}}}) ->
    DataSize = case HPTS andalso dalmatiner_opt:hpts(Bucket) of
                   true ->
                       ?DATA_SIZE * 2;
                   false ->
                       ?DATA_SIZE
                   end,
    MObj = merge_metrics(Replies),
    case needs_repair(MObj, Replies, DataSize, RR) of
        {true, RObj} ->
            %% needs_repair also checks weather we decided to drop part of the
            %% result as it is too new (when partial is passed), the new
            %% object is already returned as RObj
            ddb_counter:inc(<<"read_repair">>),
            repair(Time, Entity, RObj, Replies),
            {stop, normal, SD};
        {false, _} ->
            {stop, normal, SD}
    end;
finalize(timeout, SD=#state{system = event,
                            replies = Replies,
                            entity = {Bucket, _Time}}) ->
    repair_events(Bucket, Replies),
    {stop, normal, SD}.

handle_info(_Info, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop, badmsg, StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================
-spec save_reply(partition(), term(), state()) -> state().
save_reply(IdxNode, Obj, SD = #state{system = event, replies = Replies0}) ->
    Replies = [{IdxNode, Obj} | Replies0],
    SD#state{replies = Replies};
save_reply(IdxNode, Metrics,
           SD = #state{system = metric, replies = Replies0}) ->
    Replies = [{IdxNode, decompress(Metrics, SD)} | Replies0],
    SD#state{replies = Replies}.

-spec merge(state(), [E]) -> E.
merge(#state{system = event}, Events) ->
    merge_events(Events);
merge(#state{system = metric}, Metrics) ->
    merge_metrics(Metrics).

merge_events(Events) ->
    Merged = lists:foldl(fun({_, O}, Set) ->
                                 sets:union(Set, O)
                         end, sets:new(), Events),
    lists:sort(sets:to_list(Merged)).

repair_events(Bucket, Events) ->
    Merged = lists:foldl(fun({_, O}, Set) ->
                                 sets:union(Set, O)
                         end, sets:new(), Events),
    case needs_repair(Merged, Events) of
        true ->
            repair_events(Bucket, Merged, Events);
        false ->
            ok
    end.

repair_events(_Bucket, _Merged, []) ->
    ok;
repair_events(Bucket, Merged, [{IdxNode, Es} | R]) ->
    Missing = sets:subtract(Merged, Es),
    case sets:size(Missing) of
        0 ->
            ok;
        _ ->
            L = lists:sort(sets:to_list(Missing)),
            event_vnode:repair(IdxNode, Bucket, L)
    end,
    repair_events(Bucket, Merged, R).

%% @pure
%%
%% @doc Given a list of `Replies' return the merged value.

-spec merge_metrics([reply_src()]) -> metric_element() | not_found.
merge_metrics(Replies) ->
    case [Data || {_IdxNode, Data} <- Replies, is_binary(Data)] of
        [] ->
            not_found;
        [DH | DT] ->
            lists:foldl(fun mmath_bin:merge/2, DH, DT)
    end.


%% Snappy :(
-spec decompress(binary(), state()) -> binary().
decompress(D, #state{compression = snappy}) ->
    case snappiest:is_valid(D) of
        true ->
            {ok, Res} = snappiest:decompress(D),
            Res;
        _ ->
            D
    end;
decompress(D, #state{compression = none}) ->
    D.

%% @pure
%%
%% @doc Reconcile conflicts among conflicting values.
-spec reconcile([A :: ordsets:ordset()]) -> A :: ordsets:ordset().

reconcile(Vals) ->
    merge_metrics(Vals).

%% @pure
%%
%% @doc Given the merged object `MObj' and a list of `Replies'
%% determine if repair is needed.
needs_repair(MObj, Replies, _DataSize, true) ->
    Objs = [Obj || {_IdxNode, Obj} <- Replies],
    {lists:any(different(MObj), Objs), MObj};

needs_repair(not_found, _Replies, _DataSize, {partial, _N}) ->
    {false, undefined};
needs_repair(MObj, Replies, DataSize, {partial, N}) ->
    case byte_size(MObj) - N * DataSize of
        Keep when Keep > 0 ->
            <<Head:Keep/binary, _/binary>> = MObj,
            Objs = [Obj ||
                       {_IdxNode, <<Obj:Keep/binary, _/binary>>} <- Replies],
            {lists:any(different(Head), Objs), Head};
        _ ->
            {false, undefined}
    end.


needs_repair(MObj, Replies) ->
    Objs = [Obj || {_IdxNode, Obj} <- Replies],
    lists:any(different(MObj), Objs).

%% @pure
different(A) -> fun(B) -> A =/= B end.

%% @impure
%%
%% @doc Repair any vnodes that do not have the correct object.

%% I giveup :/
-dialyzer({nowarn_function, repair/4}).

repair(_, _, _, []) -> ok;
repair(_, _, not_found, _) -> ok;
repair(_, _, Events, _) when is_list(Events) -> ok;
repair(Time, {Bkt, {Met, _}} = MetAndTime, MObj, [{IdxNode, Obj}|T])
  when not is_list(Obj)->
    case MObj == Obj of
        true ->
            repair(Time, MetAndTime, MObj, T);
        false ->
            metric_vnode:repair(IdxNode, {Bkt, Met}, {Time, MObj}),
            repair(Time, MetAndTime, MObj, T)
    end.

%% pure
%%
%% @doc Given a list return the set of unique values.
-spec unique([A::any()]) -> [A::any()].
unique(L) ->
    sets:to_list(sets:from_list(L)).

mk_reqid() ->
    erlang:unique_integer().

get_rr_opt(Opts) ->
    case proplists:get_value(rr, Opts) of
        off ->
            {ok, false};
        on ->
            {ok, true};
        {partial, V} when is_integer(V), V > 0 ->
            {ok, {partial, V}};
        V when V =:= undefined; V =:= default ->
            {ok, true}
    end.

get_n_opt(Opts) ->
    case proplists:get_value(n, Opts) of
        undefined ->
            application:get_env(dalmatiner_db, n);
        Value ->
            {ok, Value}
    end.

get_r_opt(Opts, N) ->
    case proplists:get_value(r, Opts) of
        n ->
            {ok, N};
        R when is_integer(R), R > 0, R =< N ->
            {ok, R};
        V when V =:= undefined; V =:= default ->
            application:get_env(dalmatiner_db, r)
    end.

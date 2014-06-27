-module(metric_vnode).
-behaviour(riak_core_vnode).

-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         repair/3,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_info/2,
         handle_exit/3]).

-define(WEEK, 604800). %% Seconds in a week.
-export([put/4, get/4]).

-ignore_xref([
              start_vnode/1,
              put/4,
              get/4,
              repair/4,
              handle_info/2,
              repair/3
             ]).

-record(state, {
          partition,
          node,
          mstore,
          tbl,
          cnt_tbl,
          ct = 10
         }).

-define(MASTER, metric_vnode_master).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    P = list_to_atom(integer_to_list(Partition)),
    CT = case application:get_env(metric_vnode, cache_points) of
             {ok, V} ->
                 V;
             _ ->
                 10
         end,
    {ok, #state { partition = Partition,
                  node = node(),
                  mstore = new_store(Partition),
                  tbl = ets:new(P, [public, bag]),
                  cnt_tbl = ets:new(P, [public, set]),
                  ct = CT
                }}.

repair(IdxNode, Metric, {Time, Obj}) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Metric, Time, Obj},
                                   ignore,
                                   ?MASTER).


put(Preflist, ReqID, Metric, {Time, Values}) when is_list(Values) ->
    put(Preflist, ReqID, Metric, {Time, << <<1, V:64/signed-integer>> || V <- Values >>});

put(Preflist, ReqID, Metric, {Time, Value}) when is_integer(Value) ->
    put(Preflist, ReqID, Metric, {Time, <<1, Value:64/signed-integer>>});

put(Preflist, ReqID, Metric, {Time, Value}) ->
    riak_core_vnode_master:command(Preflist,
                                   {put, ReqID, Metric, {Time, Value}},
                                   {fsm, undefined, self()},
                                   ?MASTER).

get(Preflist, ReqID, Metric, {Time, Count}) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Metric, {Time, Count}},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({repair, Metric, Time, Value}, _Sender,
               State) ->
    MSet1 = flush_metric(State, Metric),
    MSet2 = mstore:put(MSet1, Metric, Time, Value),
    {noreply, State#state{mstore=MSet2}};

handle_command({put, {ReqID, _}, Metric, {Time, Value}}, _Sender,
               #state{tbl=T, cnt_tbl=C} = State) ->
    ets:insert(T, {Metric, Time, Value}),
    case ets:lookup(C, Metric) of
        [{Metric, Cnt}] when Cnt == State#state.ct ->
            MSet1 = flush_metric(State, Metric),
            {reply, {ok, ReqID}, State#state{mstore=MSet1}};
        [] ->
            ets:insert(C, {Metric, 0}),
            {reply, {ok, ReqID}, State};
        _ ->
            ets:update_counter(C, Metric, 1),
            {reply, {ok, ReqID}, State}
    end;

handle_command({get, ReqID, Metric, {Time, Count}}, _Sender,
               #state{partition=Partition, node=Node} = State) ->
    MSet1 = flush_metric(State, Metric),
    {ok, Data} = mstore:get(MSet1, Metric, Time, Count),
    {reply, {ok, ReqID, {Partition, Node}, Data}, State#state{mstore=MSet1}};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

flush_metric(#state{mstore=MSet, tbl=Tbl, cnt_tbl=C}, Metric) ->
    case lists:sort(ets:lookup(Tbl, Metric)) of
        [] ->
            MSet;
        [{_, T, V} | R] ->
            ets:insert(C, {Metric, 0}),
            ets:delete(Tbl, Metric),
            push_metric(MSet, Metric, T, V, T + mmath_bin:length(V), R)
    end.

push_metric(MSet, Metric, T, V, _, []) ->
    mstore:put(MSet, Metric, T, V);

push_metric(MSet, Metric, T, V, N, [{_, N, V1} | R]) ->
    push_metric(MSet, Metric, T, <<V/binary, V1/binary>>,
                N + mmath_bin:length(V1), R);

push_metric(MSet, Metric, T, V, _, [{_, T1, V1} | R]) ->
    MSet1 = mstore:put(MSet, Metric, T, V),
    push_metric(MSet1, Metric, T1, V1,
                T + mmath_bin:length(V1), R).

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender,
                       State=#state{mstore=M, tbl=T}) ->
    M1 = ets:foldl(fun({Metric, Start, V}, MAcc) ->
                           mstore:put(MAcc, Metric, Start, V)
                   end, M, T),
    ets:delete_all_objects(T),
    F = fun(Metric, Time, V, AccIn) ->
                Fun(Metric, {Time, V}, AccIn)
        end,
    Acc = mstore:fold(M1, F, Acc0),
    {reply, Acc, State#state{mstore=M1}};

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State=#state{mstore=M}) ->
    {Metric, {T, V}} = binary_to_term(Data),
    M1 = mstore:put(M, Metric, T, V),
    {reply, ok, State#state{mstore = M1}}.

encode_handoff_item(Key, Value) ->
    term_to_binary({Key, Value}).

is_empty(State = #state{tbl = T}) ->
    {gb_sets:size(mstore:metrics(State#state.mstore)) == 0 andalso
     ets:first(T) == '$end_of_table', State}.

delete(State = #state{partition=Partition, tbl=T}) ->
    ets:delete_all_objects(T),
    mstore:delete(State#state.mstore),
    {ok, State#state{mstore=new_store(Partition)}}.

handle_coverage(metrics, _KeySpaces, _Sender,
                State = #state{mstore=M, partition=Partition, node=Node,
                               tbl=T}) ->
    M1 = ets:foldl(fun({Metric, Start, _, V}, MAcc) ->
                           mstore:put(MAcc, Metric, Start, V)
                   end, M, T),
    ets:delete_all_objects(T),
    Ms =  mstore:metrics(M1),
    Reply = {ok, undefined, {Partition, Node}, Ms},
    {reply, Reply, State#state{mstore=M1}};

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_info(_Msg, State)  ->
    {ok, State}.

handle_exit(_PID, _Reason, State) ->
    {noreply, State}.

terminate(_Reason,  #state{tbl = T, mstore=M}) ->
    M1 = ets:foldl(fun({Metric, Start, _, V}, MAcc) ->
                           mstore:put(MAcc, Metric, Start, V)
                  end, M, T),
    ets:delete_all_objects(T),
    mstore:close(M1),
    ok.

new_store(Partition) ->
    DataDir = case application:get_env(riak_core, platform_data_dir) of
                  {ok, DD} ->
                      DD;
                  _ ->
                      "data"
              end,
    PartitionDir = [DataDir | [$/ | integer_to_list(Partition)]],
    CHashSize = case application:get_env(metric_vnode, chash_size) of
                    {ok, CHS} ->
                        CHS;
                    _ ->
                        8
                end,
    PointsPerFile = case application:get_env(metric_vnode, points_per_file) of
                        {ok, PPF} ->
                            PPF;
                        _ ->
                            ?WEEK
                    end,
    {ok, MSet} = mstore:new(CHashSize, PointsPerFile, PartitionDir),
    MSet.

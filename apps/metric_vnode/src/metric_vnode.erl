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
-export([mput/3, put/4, get/4]).

-ignore_xref([
              start_vnode/1,
              put/4,
              mput/3,
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
          ct
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
                  tbl = ets:new(P, [public, ordered_set]),
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
                                   {raw, undefined, self()},
                                   ?MASTER).

mput(Preflist, ReqID, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {mput, ReqID, Data},
                                   {raw, undefined, self()},
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
               #state{mstore=MSet, tbl=T}=State) ->
    MSet1 = case ets:lookup(T, Metric) of
                [{Metric, Start, _Time, V}] ->
                    ets:delete(T, Metric),
                    mstore:put(MSet, Metric, Start, V);
                _ ->
                    MSet
            end,
    MSet2 = mstore:put(MSet1, Metric, Time, Value),
    {noreply, State#state{mstore=MSet2}};

handle_command({mput, {ReqID, _}, Data}, _Sender,
               #state{mstore=MSet, tbl=T} = State) ->
    MSet1 = lists:foldl(fun({Metric, Time, Value}, MAcc) ->
                                do_put(T, MAcc, State#state.ct,Metric, Time, Value)
                        end, MSet, Data),
    {reply, {ok, ReqID}, State#state{mstore=MSet1}};

handle_command({put, {ReqID, _}, Metric, {Time, Value}}, _Sender,
               #state{mstore=MSet, tbl=T} = State) ->
    MSet1 = do_put(T, MSet, State#state.ct, Metric, Time, Value),
    {reply, {ok, ReqID}, State#state{mstore=MSet1}};

handle_command({get, ReqID, Metric, {Time, Count}}, _Sender,
               #state{mstore=MSet, partition=Partition, node=Node, tbl=T} = State) ->
    MSet1 = case ets:lookup(T, Metric) of
                [{Metric, Start, _Time, V}] ->
                    ets:delete(T, Metric),
                    mstore:put(MSet, Metric, Start, V);
                _ ->
                    MSet
            end,
    {ok, Data} = mstore:get(MSet1, Metric, Time, Count),
    {reply, {ok, ReqID, {Partition, Node}, Data}, State#state{mstore=MSet1}};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender,
                       State=#state{mstore=M, tbl=T}) ->
    M1 = ets:foldl(fun({Metric, Start, _, V}, MAcc) ->
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

do_put(T, MSet, CT, Metric, Time, Value) ->
    Len = mmath_bin:length(Value),
    case ets:lookup(T, Metric) of
        [{Metric, Start, Time, V}]
          when (Time - Start) < CT ->
            ets:update_element(T, Metric,
                               [{3, Time + Len},
                                {4, <<V/binary, Value/binary>>}]),
            MSet;
        [{Metric, Start, _Time, V}]
          when Start == (Time + Len) ->
            ets:update_element(T, Metric,
                               [{2, Time},
                                {4, <<Value/binary, V/binary>>}]),
            MSet;
        [{Metric, Start, Time, V}] ->
            ets:delete(T, Metric),
            mstore:put(MSet, Metric, Start, <<V/binary, Value/binary>>);
        [{Metric, Start, _, V}] ->
            ets:update_element(T, Metric,
                               [{2,Time},
                                {3, Time + Len},
                                {4, Value}]),
            mstore:put(MSet, Metric, Start, V);
        [] ->
            ets:insert(T, {Metric, Time, Time + Len, Value}),
            MSet
    end.

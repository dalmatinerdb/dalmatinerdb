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
          tab
         }).

-define(MASTER, metric_vnode_master).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, #state { partition=Partition,
                  node=node(),
                  mstore=new_store(Partition)}}.

repair(IdxNode, Metric, {Time, Obj}) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Metric, Time, Obj},
                                   ignore,
                                   ?MASTER).

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
               #state{mstore=MSet}=State) ->
    MSet1 = mstore:put(MSet, Metric, Time, Value),
    {noreply, State#state{mstore=MSet1}};

handle_command({put, {ReqID, _}, Metric, {Time, Value}}, _Sender,
               #state{mstore=MSet} = State) ->
    MSet1 = mstore:put(MSet, Metric, Time, Value),
    {reply, {ok, ReqID}, State#state{mstore=MSet1}};

handle_command({get, ReqID, Metric, {Time, Count}}, _Sender,
               #state{mstore=MSet, partition=Partition, node=Node} = State) ->
    {ok, Data} = mstore:get(MSet, Metric, Time, Count),
    {reply, {ok, ReqID, {Partition, Node}, Data}, State};


handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State=#state{mstore=M}) ->
    F = fun(Metric, T, V, AccIn) ->
                Fun(Metric, {T, V}, AccIn)
        end,
    Acc = mstore:fold(M, F, Acc0),
    {reply, Acc, State};

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

is_empty(State) ->
    {gb_sets:size(mstore:metrics(State#state.mstore)) == 0, State}.

delete(State = #state{partition=Partition}) ->
    mstore:delete(State#state.mstore),
    {ok, State#state{mstore=new_store(Partition)}}.

handle_coverage(metrics, _KeySpaces, _Sender,
                State = #state{mstore=M, partition=Partition, node=Node}) ->
    Ms =  mstore:metrics(M),
    Reply = {ok, undefined, {Partition, Node}, Ms},
    {reply, Reply, State};

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_info(_Msg, State)  ->
    {ok, State}.

handle_exit(_PID, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
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

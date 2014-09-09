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
-export([mput/3, put/5, get/4]).

-ignore_xref([
              start_vnode/1,
              put/5,
              mput/3,
              get/4,
              repair/4,
              handle_info/2,
              repair/3
             ]).

-record(state, {
          partition,
          node,
          mstore=gb_trees:empty(),
          tbl,
          ct,
          dir
         }).

-define(MASTER, metric_vnode_master).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    random:seed(now()),
    P = list_to_atom(integer_to_list(Partition)),
    CT = case application:get_env(metric_vnode, cache_points) of
             {ok, V} ->
                 V;
             _ ->
                 10
         end,
    DataDir = case application:get_env(riak_core, platform_data_dir) of
                  {ok, DD} ->
                      DD;
                  _ ->
                      "data"
              end,
    PartitionDir = [DataDir, $/,  integer_to_list(Partition)],
    {ok, #state { partition = Partition,
                  node = node(),
                  tbl = ets:new(P, [public, ordered_set]),
                  ct = CT,
                  dir = PartitionDir
                }}.

repair(IdxNode, {Bucket, Metric}, {Time, Obj}) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Bucket, Metric, Time, Obj},
                                   ignore,
                                   ?MASTER).


put(Preflist, ReqID, Bucket, Metric, {Time, Values}) when is_list(Values) ->
    put(Preflist, ReqID, Bucket, Metric, {Time, << <<1, V:64/signed-integer>> || V <- Values >>});

put(Preflist, ReqID, Bucket, Metric, {Time, Value}) when is_integer(Value) ->
    put(Preflist, ReqID, Bucket, Metric, {Time, <<1, Value:64/signed-integer>>});

put(Preflist, ReqID, Bucket, Metric, {Time, Value}) ->
    riak_core_vnode_master:command(Preflist,
                                   {put, Bucket, Metric, {Time, Value}},
                                   {raw, ReqID, self()},
                                   ?MASTER).

mput(Preflist, ReqID, Data) ->
    riak_core_vnode_master:command(Preflist,
                                   {mput, Data},
                                   {raw, ReqID, self()},
                                   ?MASTER).

get(Preflist, ReqID, {Bucket, Metric}, {Time, Count}) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Bucket, Metric, {Time, Count}},
                                   {fsm, undefined, self()},
                                   ?MASTER).

%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({repair, Bucket, Metric, Time, Value}, _Sender, #state{tbl=T}=State) ->
    State1 = case ets:lookup(T, {Bucket, Metric}) of
                 [{{Bucket,Metric}, Start, Size, _Time, Array}] ->
                     Bin = k6_bytea:get(Array, 0, Size * 9),
                     k6_bytea:delete(Array),
                     ets:delete(T, {Bucket,Metric}),
                     do_write(Bucket, Metric, Start, Bin, State);
                 _ ->
                     State
             end,
    State2 = do_put(Bucket, Metric, Time, Value, State1),
    {noreply, State2};

handle_command({mput, Data}, _Sender, State) ->
    State1 = lists:foldl(fun({Bucket, Metric, Time, Value}, SAcc) ->
                                 do_put(Bucket, Metric, Time, Value, SAcc)
                         end, State, Data),
    {reply, ok, State1};

handle_command({put, Bucket, Metric, {Time, Value}}, _Sender, State) ->
    State1 = do_put(Bucket, Metric, Time, Value, State),
    {reply, ok, State1};

handle_command({get, ReqID, Bucket, Metric, {Time, Count}}, _Sender,
               #state{partition=Partition, node=Node, tbl=T} = State) ->
    BM = {Bucket, Metric},
    State1 = case ets:lookup(T, BM) of
                 [{BM, Start, Size, _Time, Array}] ->
                     Bin = k6_bytea:get(Array, 0, Size * 9),
                     k6_bytea:delete(Array),
                     ets:delete(T, {Bucket,Metric}),
                     do_write(Bucket, Metric, Start, Bin, State);
                 _ ->
                     State
             end,
    {D, State2} = case get_set(Bucket, State1) of
                      {ok, {{Resolution, MSet}, S2}} ->
                          {ok, Data} = mstore:get(MSet, Metric, Time, Count),
                          {{Resolution, Data}, S2};
                      _ ->
                          Resolution = dalmatiner_opt:get(
                                         <<"buckets">>, Bucket, <<"resolution">>,
                                         {metric_vnode, resolution}, 1000),
                          {{Resolution, mmath_bin:empty(Count)}, State1}
                  end,
    {reply, {ok, ReqID, {Partition, Node}, D}, State2};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender,
                       State=#state{tbl=T}) ->
    State1 = ets:foldl(fun({{Bucket, Metric}, Start, Size, _, Array}, SAcc) ->
                               Bin = k6_bytea:get(Array, 0, Size * 9),
                               k6_bytea:delete(Array),
                               do_write(Bucket, Metric, Start, Bin, SAcc)
                       end, State, T),
    ets:delete_all_objects(T),
    Ts = gb_trees:to_list(State1#state.mstore),
    Acc = lists:foldl(fun({Bucket, {_, MStore}}, AccL) ->
                              F = fun(Metric, Time, V, AccIn) ->
                                          Fun({Bucket, Metric}, {Time, V}, AccIn)
                                  end,
                              mstore:fold(MStore, F, AccL)
                      end, Acc0, Ts),
    {reply, Acc, State1};

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    {{Bucket, Metric}, {T, V}} = binary_to_term(Data),
    State1 = do_put(Bucket, Metric, T, V, State),
    {reply, ok, State1}.

encode_handoff_item(Key, Value) ->
    term_to_binary({Key, Value}).

is_empty(State = #state{tbl = T}) ->
    R = ets:first(T) == '$end_of_table' andalso
        calc_empty(gb_trees:iterator(State#state.mstore)),
    {R, State}.

calc_empty(I) ->
    case gb_trees:next(I) of
        none ->
            true;
        {_, {_, MSet}, I2} ->
            gb_sets:is_empty(mstore:metrics(MSet))
                andalso calc_empty(I2)
    end.

delete(State = #state{partition=Partition, tbl=T}) ->
    ets:delete_all_objects(T),
    DataDir = case application:get_env(riak_core, platform_data_dir) of
                  {ok, DD} ->
                      DD;
                  _ ->
                      "data"
              end,
    PartitionDir = [DataDir, $/,  integer_to_list(Partition)],
    gb_trees:map(fun(Bucket, {_, MSet}) ->
                         mstore:delete(MSet),
                         file:del_dir([PartitionDir, $/, Bucket])
                 end, State#state.mstore),
    {ok, State#state{mstore=gb_trees:empty()}}.

handle_coverage({metrics, Bucket}, _KeySpaces, _Sender,
                State = #state{partition=Partition, node=Node, tbl=T}) ->
    State1 = ets:foldl(fun({{Bkt, Metric}, Start, Size, _, Array}, SAcc) ->
                               Bin = k6_bytea:get(Array, 0, Size * 9),
                               k6_bytea:delete(Array),
                               do_write(Bkt, Metric, Start, Bin, SAcc)
                       end, State, T),
    ets:delete_all_objects(T),
    {Ms, State2} = case get_set(Bucket, State1) of
                       {ok, {{_, M}, S2}} ->
                           {mstore:metrics(M), S2};
                       _ ->
                           {gb_sets:new(), State1}
                   end,
    Reply = {ok, undefined, {Partition, Node}, Ms},
    {reply, Reply, State2};

handle_coverage(list, _KeySpaces, _Sender,
                State = #state{partition=Partition, node=Node, tbl=T}) ->
    State1 = ets:foldl(fun({{Bkt, Metric}, Start, Size, _, Array}, SAcc) ->
                               Bin = k6_bytea:get(Array, 0, Size * 9),
                               k6_bytea:delete(Array),
                               do_write(Bkt, Metric, Start, Bin, SAcc)
                       end, State, T),
    ets:delete_all_objects(T),
    DataDir = case application:get_env(riak_core, platform_data_dir) of
                  {ok, DD} ->
                      DD;
                  _ ->
                      "data"
              end,
    PartitionDir = [DataDir, $/,  integer_to_list(Partition)],
    Buckets1 = case file:list_dir(PartitionDir) of
                   {ok, Buckets} ->
                       gb_sets:from_list([list_to_binary(B) || B <- Buckets]);
                   _ ->
                       gb_sets:new()
               end,
    Reply = {ok, undefined, {Partition, Node}, Buckets1},
    {reply, Reply, State1};

handle_coverage({delete, Bucket}, _KeySpaces, _Sender,
                State = #state{partition=Partition, node=Node, tbl=T, dir=Dir}) ->
    State1 = ets:foldl(fun({{Bkt, Metric}, Start, Size, _, Array}, SAcc) ->
                               Bin = k6_bytea:get(Array, 0, Size * 9),
                               k6_bytea:delete(Array),
                               do_write(Bkt, Metric, Start, Bin, SAcc)
                       end, State, T),
    ets:delete_all_objects(T),
    {R, State2} = case get_set(Bucket, State1) of
                      {ok, {{_, MSet}, S1}} ->
                          mstore:delete(MSet),
                          file:del_dir([Dir, $/, Bucket]),
                          MStore = gb_trees:delete(Bucket, S1#state.mstore),
                          {ok, S1#state{mstore = MStore}};
                      _ ->
                          {not_found, State}
                  end,
    Reply = {ok, undefined, {Partition, Node}, R},
    {reply, Reply, State2};

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_info(_Msg, State)  ->
    {ok, State}.

handle_exit(_PID, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, State=#state{tbl = T}) ->
    State1 = ets:foldl(fun({{Bucket, Metric}, Start, Size, _, Array}, SAcc) ->
                               Bin = k6_bytea:get(Array, 0, Size * 9),
                               k6_bytea:delete(Array),
                               do_write(Bucket, Metric, Start, Bin, SAcc)
                       end, State, T),
    ets:delete_all_objects(T),
    gb_trees:map(fun(_, {_, MSet}) ->
                         mstore:close(MSet)
                 end, State1#state.mstore),
    ok.

new_store(Partition, Bucket) ->
    DataDir = dalmatiner_opt:get(<<"buckets">>, Bucket, <<"data_dir">>,
                                 {riak_core, platform_data_dir}, "data"),
    PartitionDir = [DataDir | [$/ |  integer_to_list(Partition)]],
    BucketDir = [PartitionDir, [$/ | binary_to_list(Bucket)]],
    file:make_dir(PartitionDir),
    file:make_dir(BucketDir),
    PointsPerFile = dalmatiner_opt:get(<<"buckets">>, Bucket,
                                       <<"points_per_file">>,
                                       {metric_vnode, points_per_file}, ?WEEK),
    Resolution = dalmatiner_opt:get(<<"buckets">>, Bucket, <<"resolution">>,
                                    {metric_vnode, resolution}, 1000),
    {ok, MSet} = mstore:new(PointsPerFile, BucketDir),
    {Resolution, MSet}.

do_put(Bucket, Metric, Time, Value, State = #state{tbl = T, ct = CT}) ->
    Len = mmath_bin:length(Value),
    BM = {Bucket, Metric},
    case ets:lookup(T, BM) of
        %% If the data is before the first package in the cache we just don't
        %% cache it this way we prevent overwriting already written data.
        [{BM, _Start, _Size, _End, _V}]
          when Time < _Start ->
            do_write(Bucket, Metric, Time, Value, State);
        %% When the Delta of start time and this package is greater then the
        %% cache time we flush the cache and start a new cache with a new
        %% package
        [{BM, Start, Size, _End, Array}]
          when (Time + Len) >= _End, Len < CT ->
            Bin = k6_bytea:get(Array, 0, Size * 9),
            k6_bytea:set(Array, 0, Value),
            k6_bytea:set(Array, Len * 9, <<0:(9 * 8 * (CT - Len))>>),
            ets:update_element(T, BM, [{2, Time}, {3, Len}]),
            do_write(Bucket, Metric, Start, Bin, State);
        %% In the case the data is already longer then the cache we flush the
        %% cache
        [{BM, Start, Size, _End, Array}]
          when (Time + Len) >= _End ->
            ets:delete(T, {Bucket,Metric}),
            Bin = k6_bytea:get(Array, 0, Size * 9),
            k6_bytea:delete(Array),
            State1 = do_write(Bucket, Metric, Start, Bin, State),
            do_write(Bucket, Metric, Time, Value, State1);
        [{BM, Start, _Size, _End, Array}] ->
            Idx = Time - Start,
            k6_bytea:set(Array, Idx * 9, Value),
            ets:update_element(T, BM, [{3, Idx + Len}]),
            State;
        %% We don't have a cache yet and our data is smaller then
        %% the current cache limit
        [] when Len < CT ->
            Array = k6_bytea:new(CT*9),
            k6_bytea:set(Array, 0, Value),
            ets:insert(T, {BM, Time, Len, Time + CT, Array}),
            State;
        %% If we don't have a cache but our data is too big for the
        %% cache we happiely write it directly
        [] ->
            do_write(Bucket, Metric, Time, Value, State)
    end.

do_write(Bucket, Metric, Time, Value, State) ->
    {{R, MSet}, State1} = get_or_create_set(Bucket, State),
    MSet1 = mstore:put(MSet, Metric, Time, Value),
    Store1 = gb_trees:update(Bucket, {R, MSet1}, State1#state.mstore),
    State1#state{mstore=Store1}.

get_set(Bucket, State=#state{mstore=Store}) ->
    case gb_trees:lookup(Bucket, Store) of
        {value, MSet} ->
            {ok, {MSet, State}};
        none ->
            case bucket_exists(State#state.partition, Bucket) of
                true ->
                    R = new_store(State#state.partition, Bucket),
                    Store1 = gb_trees:insert(Bucket, R, Store),
                    {ok, {R, State#state{mstore=Store1}}};
                _ ->
                    {error, not_found}
            end
    end.

get_or_create_set(Bucket, State=#state{mstore=Store}) ->
    case get_set(Bucket, State) of
        {ok, R} ->
            R;
        {error, not_found} ->
            MSet = new_store(State#state.partition, Bucket),
            Store1 = gb_trees:insert(Bucket, MSet, Store),
            {MSet, State#state{mstore=Store1}}
    end.

bucket_exists(Partition, Bucket) ->
    DataDir = case application:get_env(riak_core, platform_data_dir) of
                  {ok, DD} ->
                      DD;
                  _ ->
                      "data"
              end,
    PartitionDir = [DataDir | [$/ |  integer_to_list(Partition)]],
    BucketDir = [PartitionDir, [$/ | binary_to_list(Bucket)]],
    filelib:is_dir(BucketDir).

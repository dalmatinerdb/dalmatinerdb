-module(metric_vnode).
-behaviour(riak_core_vnode).

-include_lib("riak_core/include/riak_core_vnode.hrl").
-include_lib("mmath/include/mmath.hrl").


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
          tbl,
          ct,
          io,
          now,
          resolutions = btrie:new(),
          lifetimes  = btrie:new()
         }).

-define(MASTER, metric_vnode_master).
-define(MAX_Q_LEN, 20).
-define(VAC, 1000*60*60*2).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, {Bucket, Metric}, {Time, Obj}) ->
    riak_core_vnode_master:command(
      IdxNode,
      {repair, Bucket, Metric, Time, Obj},
      ignore,
      ?MASTER).


put(Preflist, ReqID, Bucket, Metric, {Time, Values}) when is_list(Values) ->
    put(Preflist, ReqID, Bucket, Metric,
        {Time, << <<?INT:?TYPE_SIZE, V:?BITS/?INT_TYPE>> || V <- Values >>});

put(Preflist, ReqID, Bucket, Metric, {Time, Value}) when is_integer(Value) ->
    put(Preflist, ReqID, Bucket, Metric,
        {Time, <<?INT:?TYPE_SIZE, Value:?BITS/?INT_TYPE>>});

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


init([Partition]) ->
    ok = dalmatiner_vacuum:register(),
    process_flag(trap_exit, true),
    random:seed(erlang:phash2([node()]),
                erlang:monotonic_time(),
                erlang:unique_integer()),
    P = list_to_atom(integer_to_list(Partition)),
    CT = case application:get_env(metric_vnode, cache_points) of
             {ok, V} ->
                 V;
             _ ->
                 10
         end,
    WorkerPoolSize = case application:get_env(metric_vnode, async_workers) of
                         {ok, Val} ->
                             Val;
                         undefined ->
                             5
                     end,
    FoldWorkerPool = {pool, metric_worker, WorkerPoolSize, []},
    {ok, IO} = metric_io:start_link(Partition),
    {ok, #state{
            partition = Partition,
            node = node(),
            tbl = ets:new(P, [public, ordered_set]),
            io = IO,
            now = timestamp(),
            ct = CT
           },
     [FoldWorkerPool]}.

repair_update_cache(Bucket, Metric, Time, Count, Value,
                    #state{tbl=T} = State) ->
    case ets:lookup(T, {Bucket, Metric}) of
        %% ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐
        %% │Cache│     │     │     │     │     │Start│ ... │St+Si│     │
        %% ├─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┤
        %% │Rep. │     │Time │ ... │Ti+Co│     │     │     │     │     │
        %% └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘
        %%
        %% If we repair on a place before the metric, we will just
        %% write it!
        [{{Bucket, Metric}, Start, _Size, _Time, _Array}]
          when Time + Count < Start ->
            metric_io:write(State#state.io, Bucket, Metric, Time,
                            Value),
            State;
        %% ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐
        %% │Cache│     │Start│ ... │St+Si│     │     │     │     │     │
        %% ├─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┤
        %% │Rep. │     │     │     │     │     │Time │ ... │Ti+Co│     │
        %% └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘
        %%
        %% The data is entirely ahead of the cache, so we flush the
        %% cache and use the repair request as new the cache.
        [{{Bucket, Metric}, Start, Size, _Time, Array}]
          when Start + Size < Time ->
            Bin = k6_bytea:get(Array, 0, Size * ?DATA_SIZE),
            k6_bytea:delete(Array),
            ets:delete(T, {Bucket, Metric}),
            metric_io:write(State#state.io, Bucket, Metric, Start,
                            Bin),
            do_put(Bucket, Metric, Time, Value, State);
        %% ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐
        %% │Cache│     │     │     │Start│ ... │St+Si│     │     │     │
        %% ├─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┼─────┤
        %% │Rep. │     │     │     │     │Time │ ... │Ti+Co│     │     │
        %% └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘
        %%
        %% Now it gets tricky. The repair intersects with the
        %% cache - this should never happen but it probably will, so
        %% it sucks!  There is no sane way to merge the values if
        %% they intersect, however we know the following:
        %% 1) A repair request, based on how it is built, will never
        %%    contain unset(empty) values.
        %% 2) A cache can be an entirely empty value or contain
        %%    empty segments.
        %% Based on that, the best approach is to flush the cache and
        %% then also to flush the repair request. This ensures that
        %% nothing will be overwritten with a empty value.
        %%
        %% - Flushing the repair once could lead to empties in the
        %%   cache overwriting non-empties from the repair.
        %% - Flushing the cache and caching the repair could lead to
        %%   new empties in the new cache from the repair now
        %%   overwriting non-empties from the previous cache.
        [{{Bucket, Metric}, Start, Size, _Time, Array}] ->
            Bin = k6_bytea:get(Array, 0, Size * ?DATA_SIZE),
            k6_bytea:delete(Array),
            ets:delete(T, {Bucket, Metric}),
            metric_io:write(State#state.io, Bucket, Metric, Start,
                            Bin),
            metric_io:write(State#state.io, Bucket, Metric, Time,
                            Value),
            State;
        %% If we had no privious cache we can safely cache the
        %% repair.
        [] ->
            do_put(Bucket, Metric, Time, Value, State)
    end.

%% Repair request are always full values not including non set values!
handle_command({repair, Bucket, Metric, Time, Value}, _Sender, State)
  when is_binary(Bucket), is_binary(Metric), is_integer(Time) ->
    Count = mmath_bin:length(Value),
    case valid_ts(Time + Count, Bucket, State) of
        {true, State1} ->
            State2 = repair_update_cache(Bucket, Metric, Time, Count, Value,
                                         State1),
            {noreply, State2};
        {false, State1} ->
            {noreply, State1}
    end;

handle_command({mput, Data}, _Sender, State) ->
    State1 = lists:foldl(fun({Bucket, Metric, Time, Value}, StateAcc)
                               when is_binary(Bucket), is_binary(Metric),
                                    is_integer(Time) ->
                                 do_put(Bucket, Metric, Time, Value, StateAcc)
                         end, State, Data),
    {reply, ok, State1};

handle_command({put, Bucket, Metric, {Time, Value}}, _Sender, State)
  when is_binary(Bucket), is_binary(Metric), is_integer(Time) ->
    State1=  do_put(Bucket, Metric, Time, Value, State),
    {reply, ok, State1};

handle_command({get, ReqID, Bucket, Metric, {Time, Count}}, Sender,
               #state{tbl=T, io=IO, partition = Idx} = State) ->
    BM = {Bucket, Metric},
    case ets:lookup(T, BM) of
        %% If our request is entirely cache we don't need to bother the IO node
        [{BM, Start, Size, _Time, Array}]
          when Start =< Time,
               (Start + Size) >= (Time + Count)
               ->
            %% How many bytes can we skip?
            SkipBytes = (Time - Start) * ?DATA_SIZE,
            Data = k6_bytea:get(Array, SkipBytes, (Count * ?DATA_SIZE)),
            {Resolution, State1} = get_resolution(Bucket, State),
            {reply, {ok, ReqID, Idx, {Resolution, Data}}, State1};
        %% The request is neither before, after nor entirely inside the cache
        %% we have to read data, but apply cached part on top of it.
        [{BM, Start, Size, _Time, Array}]
          when
              %% The window starts before the cache but ends in it
              (Time < Start andalso Time + Count >= Start)

              %% The window starts inside the cahce and ends afterwards
              orelse (Time =< Start + Size andalso Time + Count > Start + Size)
              ->
            PartStart = max(Time, Start),
            PartCount = min(Time + Count, Start + Size) - PartStart,
            SkipBytes = (PartStart - Start),
            Bin = k6_bytea:get(Array, SkipBytes, (PartCount * ?DATA_SIZE)),
            Offset = PartStart - Time,
            Part = {Offset, PartCount, Bin},
            metric_io:read_rest(IO, Bucket, Metric, Time, Count, Part, ReqID, Sender),
            {noreply, State};
        %% If we are here we know that there is either no cahce or the requested
        %% window and the cache do not overlap, so we can simply serve it from
        %% the io servers
        _ ->
            metric_io:read(IO, Bucket, Metric, Time, Count, ReqID, Sender),
            {noreply, State}
    end.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, Sender,
                       State=#state{tbl=T, io = IO}) ->
    ets:foldl(fun({{Bucket, Metric}, Start, Size, _, Array}, _) ->
                      Bin = k6_bytea:get(Array, 0, Size * ?DATA_SIZE),
                      k6_bytea:delete(Array),
                      metric_io:write(IO, Bucket, Metric, Start, Bin)
              end, ok, T),
    ets:delete_all_objects(T),
    FinishFun =
        fun(Acc) ->
                riak_core_vnode:reply(Sender, Acc)
        end,
    case metric_io:fold(IO, Fun, Acc0) of
        {ok, AsyncWork} ->
            {async, {fold, AsyncWork, FinishFun}, Sender, State};
        empty ->
            {async, {fold, fun() -> Acc0 end, FinishFun}, Sender, State}
    end;

%% We want to forward all the other handoff commands
handle_handoff_command(_Message, _Sender, State) ->
    {forward, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    {{Bucket, Metric}, ValList} = binary_to_term(Data),
    true = is_binary(Bucket),
    true = is_binary(Metric),
    State1 = lists:foldl(fun ({T, Bin}, StateAcc) ->
                                 do_put(Bucket, Metric, T, Bin, StateAcc, 2)
                         end, State, ValList),
    {reply, ok, State1}.

encode_handoff_item(Key, Value) ->
    term_to_binary({Key, Value}).

is_empty(State = #state{tbl = T, io=IO}) ->
    R = ets:first(T) == '$end_of_table' andalso metric_io:empty(IO),
    {R, State}.

delete(State = #state{io = IO, tbl=T}) ->
    ets:delete_all_objects(T),
    ok = metric_io:delete(IO),
    {ok, State}.

handle_coverage({metrics, Bucket}, _KS, Sender, State = #state{io = IO}) ->
    AsyncWork = fun() ->
                        {ok, Ms} = metric_io:metrics(IO, Bucket),
                        Ms
                end,
    FinishFun = fun(Data) ->
                        reply(Data, Sender, State)
                end,
    {async, {fold, AsyncWork, FinishFun}, Sender, State};

handle_coverage({metrics, Bucket, Prefix}, _KS, Sender,
                State = #state{io = IO}) ->
    AsyncWork = fun() ->
                        {ok, Ms} = metric_io:metrics(IO, Bucket, Prefix),
                        Ms
                end,
    FinishFun = fun(Data) ->
                        reply(Data, Sender, State)
                end,
    {async, {fold, AsyncWork, FinishFun}, Sender, State};

handle_coverage(list, _KS, Sender, State = #state{io = IO}) ->
    AsyncWork = fun() ->
                        {ok, Bs} = metric_io:buckets(IO),
                        Bs
                end,
    FinishFun = fun(Data) ->
                        reply(Data, Sender, State)
                end,
    {async, {fold, AsyncWork, FinishFun}, Sender, State};

handle_coverage({update_ttl, Bucket}, _KeySpaces, _Sender,
                State = #state{partition=P, node=N,
                               lifetimes = Lifetimes}) ->
    LT1 = btrie:erase(Bucket, Lifetimes),
    Reply = {ok, undefined, {P, N}, ok},
    {reply, Reply, State#state{lifetimes = LT1}};

handle_coverage({delete, Bucket}, _KeySpaces, _Sender,
                State = #state{partition=P, node=N, tbl=T, io = IO}) ->
    ets:foldl(fun({{Bkt, Metric}, Start, Size, _, Array}, _)
                    when Bkt /= Bucket ->
                      Bin = k6_bytea:get(Array, 0, Size * ?DATA_SIZE),
                      k6_bytea:delete(Array),
                      metric_io:write(IO, Bkt, Metric, Start, Bin);
                 ({_, _, _, _, Array}, _) ->
                      k6_bytea:delete(Array)
              end, ok, T),
    ets:delete_all_objects(T),
    R = metric_io:delete(IO, Bucket),
    Reply = {ok, undefined, {P, N}, R},
    {reply, Reply, State}.

handle_info(vacuum, State = #state{io = IO, partition = P}) ->
    lager:info("[vaccum] Starting vaccum for partution ~p.", [P]),
    {ok, Bs} = metric_io:buckets(IO),
    State1 = State#state{now = timestamp()},
    State2 =
        lists:foldl(fun (Bucket, SAcc) ->
                            case expiry(Bucket, SAcc) of
                                {infinity, SAcc1} ->
                                    SAcc1;
                                {Exp, SAcc1} ->
                                    metric_io:delete(IO, Bucket, Exp),
                                    SAcc1
                            end
                    end, State1, btrie:fetch_keys(Bs)),
    lager:info("[vaccum] Finalized vaccum for partution ~p.", [P]),
    {ok, State2};

handle_info({'EXIT', IO, normal}, State = #state{io = IO}) ->
    {ok, State};

handle_info({'EXIT', IO, E}, State = #state{io = IO}) ->
    {stop, E, State};

handle_info(_, State) ->
    {ok, State}.

handle_exit(IO, normal, State = #state{io = IO}) ->
    {ok, State};

handle_exit(IO, E, State = #state{io = IO}) ->
    {stop, E, State};

handle_exit(_PID, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, #state{tbl = T, io = IO}) ->
    ets:foldl(fun({{Bucket, Metric}, Start, Size, _, Array}, _) ->
                      Bin = k6_bytea:get(Array, 0, Size * ?DATA_SIZE),
                      k6_bytea:delete(Array),
                      metric_io:write(IO, Bucket, Metric, Start, Bin)
              end, ok, T),
    ets:delete(T),
    metric_io:close(IO),
    ok.

do_put(Bucket, Metric, Time, Value, State) ->
    do_put(Bucket, Metric, Time, Value, State, ?MAX_Q_LEN).

do_put(Bucket, Metric, Time, Value,
       State = #state{tbl = T, ct = CT, io = IO},
       Sync) when is_binary(Bucket), is_binary(Metric), is_integer(Time) ->
    Len = mmath_bin:length(Value),
    BM = {Bucket, Metric},

    %% Technically, we could still write data that falls within a range that is
    %% to be deleted by the vacuum.  See the `timestamp()' function doc.
    case valid_ts(Time + Len, Bucket, State) of
        {false, State1} ->
            State1;
        {true, State1} ->
            case ets:lookup(T, BM) of
                %% If the data is before the first package in the cache we just
                %% don't cache it this way we prevent overwriting already
                %% written data.
                [{BM, _Start, _Size, _End, _V}]
                  when Time < _Start ->
                    metric_io:write(IO, Bucket, Metric, Time, Value, Sync);
                %% When the Delta of start time and this package is greater
                %% then the cache time we flush the cache and start a new cache
                %% with a new package
                [{BM, Start, Size, _End, Array}]
                  when (Time + Len) >= _End, Len < CT ->
                    Bin = k6_bytea:get(Array, 0, Size * ?DATA_SIZE),
                    k6_bytea:set(Array, 0, Value),
                    k6_bytea:set(Array, Len * ?DATA_SIZE,
                                 <<0:(?DATA_SIZE * 8 * (CT - Len))>>),
                    ets:update_element(T, BM, [{2, Time}, {3, Len},
                                               {4, Time + CT}]),
                    metric_io:write(IO, Bucket, Metric, Start, Bin, Sync);
                %% In the case the data is already longer then the cache we
                %% flush the cache
                [{BM, Start, Size, _End, Array}]
                  when (Time + Len) >= _End ->
                    ets:delete(T, {Bucket, Metric}),
                    Bin = k6_bytea:get(Array, 0, Size * ?DATA_SIZE),
                    k6_bytea:delete(Array),
                    metric_io:write(IO, Bucket, Metric, Start, Bin, Sync),
                    metric_io:write(IO, Bucket, Metric, Time, Value, Sync);
                [{BM, Start, _Size, _End, Array}] ->
                    Idx = Time - Start,
                    k6_bytea:set(Array, Idx * ?DATA_SIZE, Value),
                    ets:update_element(T, BM, [{3, Idx + Len}]);
                %% We don't have a cache yet and our data is smaller then
                %% the current cache limit
                [] when Len < CT ->
                    Array = k6_bytea:new(CT * ?DATA_SIZE),
                    k6_bytea:set(Array, 0, Value),
                    Jitter = random:uniform(CT),
                    ets:insert(T, {BM, Time, Len, Time + Jitter, Array});
                %% If we don't have a cache but our data is too big for the
                %% cache we happiely write it directly
                [] ->
                    metric_io:write(IO, Bucket, Metric, Time, Value, Sync)
            end,
            State1
    end.

reply(Reply, {_, ReqID, _} = Sender, #state{node=N, partition=P}) ->
    riak_core_vnode:reply(Sender, {ok, ReqID, {P, N}, Reply}).

%% The timestamp is primarily used by the vacuum to remove data in accordance
%% with the bucket lifetime. The timestamp is only updated once per
%% vacuum cycle, and may not accurately reflect the current system time.
%% Although more performant than the monotonic `now()', there are nevertheless
%% performance penalties for calling this too frequently. Also, updating the
%% time on a self tick message may prevent the VNode being marked as idle by
%% riak core.
timestamp() ->
    erlang:system_time(milli_seconds).


valid_ts(TS, Bucket, State) ->
    case expiry(Bucket, State) of
        %% TODO:
        %% We ignore every data where the last point is older then the
        %% lifetime this means we could still potentially write in the
        %% past if writing batches but this is a problem for another day!
        {Exp, State1} when is_integer(Exp),
                           TS < Exp ->
            {false, State1};
        {_, State1} ->
            {true, State1}
    end.

%% Return the latest point we'd ever want to save. This is more strict
%% then the expiration we do on the data but it is strong enough for
%% our guarantee.
expiry(Bucket, State = #state{now=Now}) ->
    case get_lifetime(Bucket, State) of
        {infinity, State1} ->
            {infinity, State1};
        {LT, State1} ->
            {Res, State2} = get_resolution(Bucket, State1),
            Exp = (Now - LT) div Res,
            {Exp, State2}
    end.

get_resolution(Bucket, State = #state{resolutions = Ress}) ->
    case btrie:find(Bucket, Ress) of
        {ok, Resolution} ->
            {Resolution, State};
        error ->
            Resolution = dalmatiner_opt:resolution(Bucket),
            Ress1 = btrie:store(Bucket, Resolution, Ress),
            {Resolution, State#state{resolutions = Ress1}}
    end.

get_lifetime(Bucket, State = #state{lifetimes = Lifetimes}) ->
    case btrie:find(Bucket, Lifetimes) of
        {ok, Resolution} ->
            {Resolution, State};
        error ->
            Resolution = dalmatiner_opt:lifetime(Bucket),
            Lifetimes1 = btrie:store(Bucket, Resolution, Lifetimes),
            {Resolution, State#state{lifetimes = Lifetimes1}}
    end.

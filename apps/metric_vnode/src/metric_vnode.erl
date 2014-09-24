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
          io
         }).

-define(MASTER, metric_vnode_master).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    process_flag(trap_exit, true),
    random:seed(now()),
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
            ct = CT
           },
     [FoldWorkerPool]}.

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
    case ets:lookup(T, {Bucket, Metric}) of
        [{{Bucket,Metric}, Start, Size, _Time, Array}] ->
            Bin = k6_bytea:get(Array, 0, Size * 9),
            k6_bytea:delete(Array),
            ets:delete(T, {Bucket,Metric}),
            metric_io:write(State#state.io, Bucket, Metric, Start, Bin);
        _ ->
            ok
    end,
    do_put(Bucket, Metric, Time, Value, State),
    {noreply, State};

handle_command({mput, Data}, _Sender, State) ->
    lists:foldl(fun({Bucket, Metric, Time, Value}, _) ->
                                 do_put(Bucket, Metric, Time, Value, State)
                         end, ok, Data),
    {reply, ok, State};

handle_command({put, Bucket, Metric, {Time, Value}}, _Sender, State) ->
    do_put(Bucket, Metric, Time, Value, State),
    {reply, ok, State};

handle_command({get, ReqID, Bucket, Metric, {Time, Count}}, Sender,
               #state{tbl=T, io=IO} = State) ->
    BM = {Bucket, Metric},
    case ets:lookup(T, BM) of
        [{BM, Start, Size, _Time, Array}] ->
            Bin = k6_bytea:get(Array, 0, Size * 9),
            k6_bytea:delete(Array),
            ets:delete(T, {Bucket,Metric}),
            metric_io:write(IO, Bucket, Metric, Start, Bin);
        _ ->
            ok
    end,
    metric_io:read(IO, Bucket, Metric, Time, Count, ReqID, Sender),
    {noreply, State};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender,
                       State=#state{tbl=T, io = IO}) ->
    ets:foldl(fun({{Bucket, Metric}, Start, Size, _, Array}, _) ->
                      Bin = k6_bytea:get(Array, 0, Size * 9),
                      k6_bytea:delete(Array),
                      metric_io:write(IO, Bucket, Metric, Start, Bin)
              end, ok, T),
    ets:delete_all_objects(T),
    Acc = metric_io:fold(IO, Fun, Acc0),
    {reply, Acc, State};

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
    do_put(Bucket, Metric, T, V, State),
    {reply, ok, State}.

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

handle_coverage(list, _KS, Sender, State = #state{io = IO}) ->
    AsyncWork = fun() ->
                        {ok, Bs} = metric_io:buckets(IO),
                        Bs
                end,
    FinishFun = fun(Data) ->
                        reply(Data, Sender, State)
                end,
    {async, {fold, AsyncWork, FinishFun}, Sender, State};

handle_coverage({delete, Bucket}, _KeySpaces, _Sender,
                State = #state{partition=P, node=N, tbl=T, io = IO}) ->
    ets:foldl(fun({{Bkt, Metric}, Start, Size, _, Array}, _)
                    when Bkt /= Bucket ->
                      Bin = k6_bytea:get(Array, 0, Size * 9),
                      k6_bytea:delete(Array),
                      metric_io:write(IO, Bkt, Metric, Start, Bin);
                 ({_, _, _, _, Array}, _) ->
                      k6_bytea:delete(Array)
              end, ok, T),
    ets:delete_all_objects(T),
    R = metric_io:delete(IO, Bucket),
    Reply = {ok, undefined, {P, N}, R},

    {reply, Reply, State};

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

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
                      Bin = k6_bytea:get(Array, 0, Size * 9),
                      k6_bytea:delete(Array),
                      metric_io:write(IO, Bucket, Metric, Start, Bin)
              end, ok, T),
    ets:delete_all_objects(T),
    metric_io:close(IO),
    ok.

do_put(Bucket, Metric, Time, Value, State = #state{tbl = T, ct = CT, io = IO}) ->
    Len = mmath_bin:length(Value),
    BM = {Bucket, Metric},
    case ets:lookup(T, BM) of
        %% If the data is before the first package in the cache we just don't
        %% cache it this way we prevent overwriting already written data.
        [{BM, _Start, _Size, _End, _V}]
          when Time < _Start ->
            metric_io:write(IO, Bucket, Metric, Time, Value);
        %% When the Delta of start time and this package is greater then the
        %% cache time we flush the cache and start a new cache with a new
        %% package
        [{BM, Start, Size, _End, Array}]
          when (Time + Len) >= _End, Len < CT ->
            Bin = k6_bytea:get(Array, 0, Size * 9),
            k6_bytea:set(Array, 0, Value),
            k6_bytea:set(Array, Len * 9, <<0:(9 * 8 * (CT - Len))>>),
            ets:update_element(T, BM, [{2, Time}, {3, Len}, {4, Time + CT}]),
            metric_io:write(IO, Bucket, Metric, Start, Bin);
        %% In the case the data is already longer then the cache we flush the
        %% cache
        [{BM, Start, Size, _End, Array}]
          when (Time + Len) >= _End ->
            ets:delete(T, {Bucket,Metric}),
            Bin = k6_bytea:get(Array, 0, Size * 9),
            k6_bytea:delete(Array),
            metric_io:write(IO, Bucket, Metric, Start, Bin),
            metric_io:write(IO, Bucket, Metric, Time, Value);
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
            Jitter = random:uniform(CT),
            ets:insert(T, {BM, Time, Len, Time + Jitter, Array});
        %% If we don't have a cache but our data is too big for the
        %% cache we happiely write it directly
        [] ->
            metric_io:write(IO, Bucket, Metric, Time, Value)
    end.


reply(Reply, {_, ReqID, _} = Sender, #state{node=N, partition=P}) ->
    riak_core_vnode:reply(Sender, {ok, ReqID, {P, N}, Reply}).

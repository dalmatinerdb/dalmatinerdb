-module(event_vnode).
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
          io
         }).

-define(MASTER, event_vnode_master).
-define(MAX_Q_LEN, 20).
-define(VAC, 1000*60*60*2).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, {Bucket, Event}, {Time, Obj}) ->
    riak_core_vnode_master:command(
      IdxNode,
      {repair, Bucket, Event, Time, Obj},
      ignore,
      ?MASTER).

put(Preflist, ReqID, Bucket, Events) ->
    lager:info("Put: ~p", [[Preflist, Bucket, Events]]),
    riak_core_vnode_master:command(Preflist,
                                   {put, Bucket, Events},
                                   {raw, ReqID, self()},
                                   ?MASTER).

get(Preflist, ReqID, Bucket, {Start, End}) ->
    lager:info("Get: ~p", [[Preflist, Bucket, {Start, End}]]),
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Bucket, Start, End},
                                   {fsm, undefined, self()},
                                   ?MASTER).

init([Partition]) ->
    process_flag(trap_exit, true),
    random:seed(erlang:phash2([node()]),
                erlang:monotonic_time(),
                erlang:unique_integer()),
    WorkerPoolSize = case application:get_env(event_vnode, async_workers) of
                         {ok, Val} ->
                             Val;
                         undefined ->
                             5
                     end,
    FoldWorkerPool = {pool, event_worker, WorkerPoolSize, []},
    {ok, IO} = event_io:start_link(Partition),
    {ok, #state{
            io = IO,
            partition = Partition,
            node = node()
           },
     [FoldWorkerPool]}.

handle_command({put, Bucket, Events}, _Sender, State)
  when is_binary(Bucket), is_list(Events) ->
    State1 = do_put(Bucket, Events, State),
    {reply, ok, State1};

handle_command({get, ReqID, Bucket, Start, End}, Sender,
               State = #state{io=IO}) ->
    event_io:read(IO, Bucket, Start, End, ReqID, Sender),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, Sender,
                       State=#state{io = IO}) ->
    FinishFun =
        fun(Acc) ->
                riak_core_vnode:reply(Sender, Acc)
        end,
    case event_io:fold(IO, Fun, Acc0) of
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

-dialyzer({no_return, handle_handoff_data/2}).
handle_handoff_data(Compressed, State) ->
    Data = case riak_core_capability:get({ddb, handoff}) of
               snappy ->
                   {ok, Decompressed} = snappy:decompress(Compressed),
                   Decompressed;
               _ ->
                   Compressed
           end,
    {Bucket, Events} = binary_to_term(Data),
    true = is_binary(Bucket),
    true = is_list(Events),
    State1 = do_put(Bucket, Events, State, 2),
    {reply, ok, State1}.

-dialyzer({no_return, encode_handoff_item/2}).
encode_handoff_item(Key, Value) ->
    case riak_core_capability:get({ddb, handoff}) of
        snappy ->
            {ok, R} = snappy:compress(term_to_binary({Key, Value})),
            R;
        _ ->
            term_to_binary({Key, Value})
    end.

is_empty(State = #state{io=IO}) ->
    case event_io:count(IO) of
        0 ->
            {true, State};
        Count ->
            {false, {Count, objects}, State}
    end.

delete(State = #state{io = IO}) ->
    ok = event_io:delete(IO),
    {ok, State}.

handle_coverage(_, _KeySpaces, _Sender, State = #state{partition=P, node=N}) ->
    Reply = {ok, undefined, {P, N}, []},
    {reply, Reply, State}.

handle_info({'EXIT', IO, normal}, State = #state{io = IO}) ->
    lager:info("Info: normal exit"),
    {ok, State};

handle_info({'EXIT', IO, E}, State = #state{io = IO}) ->
    {stop, E, State};

handle_info(I, State) ->
    lager:info("Info: ~p", [I]),
    {ok, State}.

handle_exit(IO, normal, State = #state{io = IO}) ->
    {ok, State};

handle_exit(IO, E, State = #state{io = IO}) ->
    {stop, E, State};

handle_exit(_PID, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, #state{io = IO}) ->
    event_io:close(IO),
    ok.

do_put(Bucket, Events, State) ->
    do_put(Bucket, Events, State, ?MAX_Q_LEN).
do_put(Bucket, Events, State = #state{io = IO}, Sync) ->
    event_io:write(IO, Bucket, Events, Sync),
    State.

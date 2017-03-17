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
         handle_overload_command/3,
         handle_overload_info/2,
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
          now,
          partition,
          lifetimes = btrie:new(),
          node,
          io
         }).

-define(MASTER, event_vnode_master).
-define(MAX_Q_LEN, 20).
-define(VAC, 1000*60*60*2).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

repair(IdxNode, Bucket, Events) ->
    riak_core_vnode_master:command(
      IdxNode,
      {put, Bucket, Events},
      ignore,
      ?MASTER).

put(Preflist, ReqID, Bucket, Events) ->
    riak_core_vnode_master:command(Preflist,
                                   {put, Bucket, Events},
                                   {raw, ReqID, self()},
                                   ?MASTER).

get(Preflist, ReqID, Bucket, {Start, End}) ->
    get(Preflist, ReqID, Bucket, {Start, End, []});
get(Preflist, ReqID, Bucket, {Start, End, Filter}) ->
    riak_core_vnode_master:command(Preflist,
                                   {get, ReqID, Bucket, Start, End, Filter},
                                   {fsm, undefined, self()},
                                   ?MASTER).

init([Partition]) ->
    process_flag(trap_exit, true),
    ok = dalmatiner_vacuum:register(),
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

handle_command({get, ReqID, Bucket, Start, End, Filter}, Sender,
               State = #state{io=IO}) ->
    event_io:read(IO, Bucket, Start, End, Filter, ReqID, Sender),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, Sender,
                       State=#state{io = IO}) ->
    FinishFun =
        fun(Acc) ->
                riak_core_vnode:reply(Sender, Acc)
        end,
    lager:debug("[handoff] begins"),
    case event_io:fold(IO, Fun, Acc0) of
        {ok, AsyncWork} ->
            lager:debug("[handoff] async"),
            {async, {fold, AsyncWork, FinishFun}, Sender, State};
        empty ->
            lager:debug("[handoff] empty"),
            {async, {fold, fun() -> Acc0 end, FinishFun}, Sender, State}
    end;

%% We want to forward all the other handoff commands
%% TODO: This is very tricky how to handle commands that come in
%% during an handoff.
handle_handoff_command({put, Bucket, Events}, _Sender, State)
  when is_binary(Bucket), is_list(Events) ->
    lager:debug("[handoff] put"),
    State1 = do_put(Bucket, Events, State),
    {reply, ok, State1};

handle_handoff_command(_Message, _Sender, State) ->
    lager:debug("[handoff] forward"),
    {forward, State}.

handoff_starting(_TargetNode, State) ->
    lager:debug("[handoff] starting"),
    {true, State}.

handoff_cancelled(State) ->
    lager:debug("[handoff] cancled"),
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    lager:debug("[handoff] finished"),
    {ok, State}.

decode_v2_handoff_data(<<02:16, Compressed/binary>>) ->
    {ok, Decompressed} = snappiest:decompress(Compressed),
    Decompressed.

handle_handoff_data(In, State) ->
    Data = case riak_core_capability:get({ddb, handoff}) of
               v2 ->
                   decode_v2_handoff_data(In);
               plain ->
                   In
           end,
    {{Bucket, Time}, {ID, Event}} = binary_to_term(Data),
    true = is_binary(Bucket),
    State1 = do_put(Bucket, [{Time, ID, Event}], State, 2),
    {reply, ok, State1}.

encode_handoff_item(Key, Value) ->
    case riak_core_capability:get({ddb, handoff}) of
        v2 ->
            {ok, R} = snappiest:compress(term_to_binary({Key, Value})),
            <<02:16, R/binary>>;
        plain ->
            term_to_binary({Key, Value})
    end.

is_empty(State = #state{io=IO}) ->
    case event_io:count(IO) of
        0 ->
            {true, State};
        Count ->
            {false, {Count, objects}, State}
    end.

delete(State = #state{io = IO, partition = P}) ->
    lager:warning("[event:~p] deleting vnode.", [P]),
    ok = event_io:delete(IO),
    {ok, State}.

handle_coverage({delete, Bucket}, _KeySpaces, _Sender,
                State = #state{partition=P, node=N, io = IO}) ->
    _Repply = event_io:delete(IO, Bucket),
    R = btrie:new(),
    R1 = btrie:store(Bucket, t, R),
    Reply = {ok, undefined, {P, N}, R1},
    {reply, Reply, State};

handle_coverage(_, _KeySpaces, _Sender, State = #state{partition=P, node=N}) ->
    Reply = {ok, undefined, {P, N}, []},
    {reply, Reply, State}.

handle_info(vacuum, State = #state{io = IO, partition = P}) ->
    lager:info("[vaccum] Starting vaccum for partution ~p.", [P]),
    {ok, Bs} = event_io:buckets(IO),
    State1 = State#state{now = timestamp()},
    State2 =
        lists:foldl(fun (Bucket, SAcc) ->
                            case expiry(Bucket, SAcc) of
                                {infinity, SAcc1} ->
                                    SAcc1;
                                {Exp, SAcc1} ->
                                    lager:debug("[vaccum:~p] expiry is: ~p",
                                                [Bucket, Exp]),
                                    event_io:delete(IO, Bucket, Exp),
                                    SAcc1
                            end
                    end, State1, btrie:fetch_keys(Bs)),
    lager:info("[vaccum] Finalized vaccum for partition ~p.", [P]),
    {ok, State2};

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

timestamp() ->
    erlang:system_time(nano_seconds).


%% Return the latest point we'd ever want to save. This is more strict
%% then the expiration we do on the data but it is strong enough for
%% our guarantee.
expiry(Bucket, State = #state{now=Now}) ->
    case get_lifetime(Bucket, State) of
        {infinity, State1} ->
            {infinity, State1};
        {LT, State1} ->
            Exp = erlang:convert_time_unit(
                    Now - LT, milli_seconds, nano_seconds),
            {Exp, State1}
    end.

get_lifetime(Bucket, State = #state{lifetimes = Lifetimes}) ->
    case btrie:find(Bucket, Lifetimes) of
        {ok, TTL} ->
            {TTL, State};
        error ->
            TTL = dalmatiner_opt:lifetime(Bucket),
            Lifetimes1 = btrie:store(Bucket, TTL, Lifetimes),
            {TTL, State#state{lifetimes = Lifetimes1}}
    end.

%% Handling other failures
handle_overload_command(_Req, Sender, Idx) ->
    riak_core_vnode:reply(Sender, {fail, Idx, overload}).

handle_overload_info(_, _Idx) ->
    ok.

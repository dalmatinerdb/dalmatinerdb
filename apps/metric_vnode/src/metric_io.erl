%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@schroedinger.local>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 24 Sep 2014 by Heinz Nikolaus Gies <heinz@schroedinger.local>
%%%-------------------------------------------------------------------
-module(metric_io).

-behaviour(gen_server).

-include_lib("mmath/include/mmath.hrl").
-include("metric_io.hrl").

%% API
-export([start_link/1, count/1, get_bitmap/6, update_env/1,
         empty/1, fold/3, delete/1, delete/2, delete/3, close/1,
         buckets/1, metrics/2, metrics/3,
         read/7, read_rest/8, write/5]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(WEEK, 604800). %% Seconds in a week.

-type entry() :: {non_neg_integer(), mstore:mstore()}.
-type read_part() :: {non_neg_integer(), pos_integer(), binary()}.
-record(state, {
          async_read = false :: boolean(),
          async_min_size = 4069 :: non_neg_integer(),
          compression :: snappy | none | undefined,
          partition,
          node,
          mstores = gb_trees:empty() :: gb_trees:tree(binary(), entry()),
          closed_mstores = gb_trees:empty() :: gb_trees:tree(binary(), entry()),
          dir,
          reader_pool :: pid() | undefined,
          pool_config :: {pos_integer(), fifo | filo},
          fold_size,
          max_open_stores
         }).

-record(io_handle, {
          pid :: pid(),
          max_async_read
          = application:get_env(metric_vnode, max_async_io, 20)
          :: non_neg_integer(),
          max_async_write
          = application:get_env(metric_vnode, max_async_io, 20)
          :: non_neg_integer(),
          read_timeout
          = application:get_env(metric_vnode, sync_timeout, 30000)
          :: non_neg_integer(),
          write_timeout
          = application:get_env(metric_vnode, sync_timeout, 30000)
          :: non_neg_integer(),
          other_timeout
          = application:get_env(metric_vnode, sync_timeout, 30000)
          :: non_neg_integer()
         }).

-opaque io_handle() :: #io_handle{}.
-export_type([io_handle/0]).

-type state() :: #state{}.

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
-spec start_link(pos_integer()) -> {ok, io_handle()} | ignore | {error, _}.

start_link(Partition) ->
    case gen_server:start_link(?MODULE, [Partition], []) of
        {ok, PID} ->
            {ok, #io_handle{pid = PID}};
        Error ->
            Error
    end.
-spec update_handle(io_handle()) -> io_handle().
update_handle(#io_handle{pid = PID}) ->
    #io_handle{pid = PID}.

update_env(Hdl) ->
    io_cast(Hdl, update_env),
    update_handle(Hdl).

-spec write(io_handle(), binary(), binary(), non_neg_integer(), binary()) ->
                   ok.
write(Hdl = #io_handle{pid = Pid, max_async_write = MaxLen},
      Bucket, Metric, Time, Value) ->
    case erlang:process_info(Pid, message_queue_len) of
        {message_queue_len, N} when N > MaxLen ->
            swrite(Hdl, Bucket, Metric, Time, Value);
        _ ->
            io_cast(Hdl, {write, Bucket, Metric, Time, Value})
    end.

-spec swrite(io_handle(), binary(), binary(), non_neg_integer(), binary()) ->
                    ok.
swrite(Hdl, Bucket, Metric, Time, Value) ->
    io_call_w(Hdl, {write, Bucket, Metric, Time, Value}).

-spec get_bitmap(io_handle(), binary(), binary(), non_neg_integer(), term(),
                 term()) ->
                        ok.
get_bitmap(Hdl, Bucket, Metric, Time, Ref, Sender) ->
    io_cast(Hdl, {get_bitmap, Bucket, Metric, Time, Ref, Sender}).

-spec read(io_handle(), binary(), binary(), non_neg_integer(),
           pos_integer(), term(), term()) ->
                  ok | {error, _}.
read(Hdl = #io_handle{pid = Pid, max_async_write = MaxLen},
     Bucket, Metric, Time, Count, ReqID, Sender) ->
    case erlang:process_info(Pid, message_queue_len) of
        {message_queue_len, N} when N > MaxLen ->
            sread(Hdl, Bucket, Metric, Time, Count, ReqID, Sender);
        _ ->
            io_cast(Hdl, {read, Bucket, Metric, Time, Count,
                          ReqID, Sender})
    end.

-spec read_rest(io_handle(), binary(), binary(), non_neg_integer(),
                pos_integer(), read_part(), term(), term()) ->
                       ok | {error, _}.
read_rest(Hdl = #io_handle{pid = Pid, max_async_write = MaxLen},
          Bucket, Metric, Time, Count, Part, ReqID, Sender) ->
    case erlang:process_info(Pid, message_queue_len) of
        {message_queue_len, N} when N > MaxLen ->
            sread_rest(Hdl, Bucket, Metric, Time, Count, Part, ReqID, Sender);
        _ ->
            io_cast(Hdl, {read_rest, Bucket, Metric, Time, Count,
                          Part, ReqID, Sender})
    end.

-spec sread(io_handle(), binary(), binary(), non_neg_integer(),
            pos_integer(), term(), term()) ->
                   ok | {error, _}.
sread(Hdl, Bucket, Metric, Time, Count, ReqID, Sender) ->
    io_call_r(Hdl, {read, Bucket, Metric, Time, Count, ReqID, Sender}).

-spec sread_rest(io_handle(), binary(), binary(), non_neg_integer(),
                 pos_integer(), read_part(), term(), term()) ->
                        ok | {error, _}.
sread_rest(Hdl, Bucket, Metric, Time, Count, Part, ReqID, Sender) ->
    io_call_r(
      Hdl, {read_rest, Bucket, Metric, Time, Count, Part, ReqID, Sender}).

count(Hdl) ->
    io_call_r(Hdl, count).

buckets(Hdl) ->
    io_call_r(Hdl, buckets).

metrics(Hdl, Bucket) ->
    io_call_r(Hdl, {metrics, Bucket}).

metrics(Hdl, Bucket, Prefix) ->
    io_call_r(Hdl, {metrics, Bucket, Prefix}).

fold(Hdl, Fun, Acc0) ->
    io_call_r(Hdl, {fold, Fun, Acc0}).

empty(Hdl) ->
    io_call_r(Hdl, empty).

delete(Hdl) ->
    io_call_w(Hdl, delete).

close(Hdl) ->
    io_call_w(Hdl, close).

delete(Hdl, Bucket) ->
    io_call_w(Hdl, {delete, Bucket}).

delete(Hdl, Bucket, Before) ->
    io_call_w(Hdl, {delete, Bucket, Before}).

-spec io_call_w(io_handle(), term()) -> term().
io_call_w(#io_handle{pid = Pid, write_timeout = Timeout}, Msg) ->
    gen_server:call(Pid, Msg, Timeout).

-spec io_call_r(io_handle(), term()) -> term().
io_call_r(#io_handle{pid = Pid, read_timeout = Timeout}, Msg) ->
    gen_server:call(Pid, Msg, Timeout).

-spec io_cast(io_handle(), term()) -> ok.
io_cast(#io_handle{pid = Pid}, Msg) ->
    gen_server:cast(Pid, Msg).

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
init([Partition]) ->
    process_flag(trap_exit, true),
    DataDir = application:get_env(riak_core, platform_data_dir, "data"),
    PartitionDir = filename:join([DataDir,  integer_to_list(Partition)]),
    PConfig = get_pool_config(),
    {ok, do_update_env(#state{partition = Partition,
                              node = node(),
                              pool_config = PConfig,
                              dir = PartitionDir})}.

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
%% fold_fun(Fun, Bucket) ->
%%     fun(Metric, Time, V,
%%         {[{_Metric2, _}|_] = AccL, AccIn})
%%           when Metric =/= _Metric2 ->
%%             AccOut = Fun({Bucket, {Metric, Time}}, V, AccIn)
%%     end.

-record(facc,
        {
          metric,
          size = 0,
          hacc,
          lacc = [],
          bucket,
          acc_fun,
          last,
          file_size,
          current_file = undefined,
          max_delta = 300,
          fold_size = 82800
        }).


fold_fun(Metric, Time, V,
         Acc =
             #facc{file_size = FileSize,
                   metric = Metric2,
                   lacc = []}) when
      Metric =/= Metric2 ->
    Size = mmath_bin:length(V),
    Acc#facc{
      metric = Metric,
      last = Time + Size,
      size = Size,
      current_file = Time div FileSize,
      lacc = [{Time, V}]};
fold_fun(Metric, Time, V,
         Acc =
             #facc{metric = Metric2,
                   bucket = Bucket,
                   lacc = AccL,
                   acc_fun = Fun,
                   file_size = FileSize,
                   current_file = CurrentFile,
                   hacc = AccIn}) when
      Metric =/= Metric2;
      CurrentFile =/= Time div FileSize ->
    Size = mmath_bin:length(V),
    AccOut = Fun({Bucket, Metric2}, lists:reverse(AccL), AccIn),
    Acc#facc{
      metric = Metric,
      last = Time + Size,
      size = Size,
      current_file = Time div FileSize,
      hacc = AccOut,
      lacc = [{Time, V}]};

fold_fun(Metric, Time, V,
         Acc =
             #facc{metric = Metric,
                   size = Size,
                   last = Last,
                   lacc = [{T0, AccE} | AccL],
                   max_delta = MaxDelta}) ->
    ThisSize = mmath_bin:length(V),
    case Time - Last of
        Delta when Delta > 0,
                   Delta =< MaxDelta ->
            AccV = <<AccE/binary, (mmath_bin:empty(Delta))/binary, V/binary>>,
            Acc#facc{
              size = Size + Delta + ThisSize,
              last = Time + ThisSize,
              lacc = [{T0, AccV} | AccL]};
        %% Otherwise delta is either 0 which should happen so rarely that it
        %% does not matter or be negative.
        %%
        %% Now this seems odd but a negative delta can happen when when
        %% multiple datafiles exists since their processing order is not
        %% guaranteed.
        _ ->
            Acc#facc{
              size = Size + ThisSize,
              last = Time + ThisSize,
              lacc = [{Time, V}, {T0, AccE} | AccL]}
    end.

bucket_fold_fun({BucketDir, Bucket}, {AccIn, Fun}) ->
    case mstore:open(BucketDir) of
        {ok, MStore} ->
            Acc1 = #facc{hacc = AccIn,
                         bucket = Bucket,
                         file_size = mstore:file_size(MStore),
                         acc_fun = Fun},
            AccOut = mstore:fold(MStore, fun fold_fun/4, Acc1),
            mstore:close(MStore),
            case AccOut of
                #facc{lacc=[], hacc=HAcc} ->
                    {HAcc, Fun};
                #facc{bucket = Bucket, metric = Metric,
                      lacc=AccL, hacc=HAcc}->
                    {Fun({Bucket, Metric}, lists:reverse(AccL), HAcc), Fun}
            end;
        {error, E} ->
            lager:warning("Empty bucket detencted going to remove it: ~s / ~p",
                          [BucketDir, E]),
            file:del_dir(BucketDir),
            {AccIn, Fun}
    end.

fold_buckets_fun(PartitionDir, Buckets, Fun, Acc0) ->
    Buckets1 = [{filename:join([PartitionDir, BucketS]),
                 list_to_binary(BucketS)}
                || BucketS <- Buckets],
    fun() ->
            {Out, _} = lists:foldl(fun bucket_fold_fun/2, {Acc0, Fun},
                                   Buckets1),
            Out
    end.

handle_call(count, _From, State = #state{dir = PartitionDir}) ->
    case file:list_dir(PartitionDir) of
        {ok, Buckets} ->
            Buckets1 = [filename:join([PartitionDir, BucketS])
                        || BucketS <- Buckets],
            Count = lists:foldl(fun(Bucket, Acc) ->
                                        Acc + mstore:count(Bucket)
                                end, 0, Buckets1),

            {reply, Count, State};
        _ ->
            {reply, 0, State}
    end;

handle_call({fold, Fun, Acc0}, _From, State = #state{dir = PartitionDir}) ->
    case file:list_dir(PartitionDir) of
        {ok, Buckets} ->
            AsyncWork = fold_buckets_fun(PartitionDir, Buckets, Fun, Acc0),
            {reply, {ok, AsyncWork}, State};
        _ ->
            {reply, empty, State}
    end;

handle_call(empty, _From, State) ->
    R = calc_empty(gb_trees:iterator(State#state.mstores)) andalso
        calc_empty(gb_trees:iterator(State#state.closed_mstores)),
    {reply, R, State};

handle_call(delete, _From, State = #state{dir = PartitionDir}) ->
    lager:warning("[metric] deleting io node: ~s.", [PartitionDir]),
    gb_trees:map(fun(Bucket, {_, MSet}) ->
                         lager:warning("[metric] deleting bucket: ~s.",
                                       [Bucket]),
                         mstore:delete(MSet),
                         file:del_dir(filename:join([PartitionDir, Bucket]))
                 end, State#state.mstores),
    gb_trees:map(fun(Bucket, {_, MSet}) ->
                         lager:warning("[metric] deleting bucket: ~s.",
                                       [Bucket]),
                         mstore:delete(MSet),
                         file:del_dir(filename:join([PartitionDir, Bucket]))
                 end, State#state.closed_mstores),
    case list_buckets(State) of
        {ok, Buckets} ->
            [case mstore:open(filename:join([PartitionDir, B])) of
                 {ok, Store}  ->
                     lager:warning("[metric] deleting bucket: ~s.",
                                   [B]),
                     mstore:delete(Store),
                     file:del_dir(filename:join([PartitionDir, B]));
                 {error, enoent} ->
                     lager:warning("[metric] deleting (empty) bucket: ~s.",
                                   [B]),
                     file:del_dir(filename:join([PartitionDir, B]))
             end || B <- Buckets];
        _ ->
            ok
    end,
    {reply, ok, State#state{mstores = gb_trees:empty(),
                            closed_mstores = gb_trees:empty()}};

handle_call(close, _From, State) ->
    gb_trees:map(fun(_, {_, MSet}) ->
                         mstore:close(MSet)
                 end, State#state.mstores),
    State1 = State#state{mstores = gb_trees:empty(),
                         closed_mstores = gb_trees:empty()},
    {reply, ok, State1};

handle_call({delete, Bucket}, _From,
            State = #state{dir = Dir}) ->
    {R, State1} = case get_set(Bucket, State) of
                      {ok, {{_, MSet}, S1}} ->
                          mstore:delete(MSet),
                          file:del_dir(filename:join([Dir, Bucket])),
                          MStore = gb_trees:delete(Bucket, S1#state.mstores),
                          CMStore = gb_trees:delete(
                                      Bucket, S1#state.closed_mstores),
                          {ok, S1#state{mstores = MStore,
                                        closed_mstores = CMStore}};
                      _ ->
                          {not_found, State}
                  end,
    {reply, R, State1};

handle_call({delete, Bucket, Before}, _From, State) ->
    {R, State1} = case get_set(Bucket, State) of
                      {ok, {{LastWritten, MSet}, S1}} ->
                          {ok, MSet1} = mstore:delete(MSet, Before),
                          V = {LastWritten, MSet1},
                          MStore = gb_trees:enter(Bucket, V, S1#state.mstores),
                          {ok, S1#state{mstores = MStore}};
                      _ ->
                          {not_found, State}
                  end,
    {reply, R, State1};

handle_call(buckets, _From, State = #state{dir = PartitionDir}) ->
    Buckets1 = case file:list_dir(PartitionDir) of
                   {ok, Buckets} ->
                       btrie:from_list([{list_to_binary(B), t}
                                        || B <- Buckets]);
                   _ ->
                       btrie:new()
               end,
    {reply, {ok, Buckets1}, State};

handle_call({metrics, Bucket}, _From, State) ->
    {Ms, State1} = case get_set(Bucket, State) of
                       {ok, {{_, M}, S2 = #state{mstores = Stores}}} ->
                           {Metrics, M1} = mstore:metrics(M),
                           Stores1 = gb_trees:enter(Bucket, M1, Stores),
                           S3 = S2#state{mstores = Stores1},
                           {Metrics, S3};
                       _ ->
                           {btrie:new(), State}
                   end,
    {reply, {ok, Ms}, State1};

handle_call({metrics, Bucket, Prefix}, _From, State) ->
    {MsR, State1} = case get_set(Bucket, State) of
                        {ok, {{_, M}, S2 = #state{mstores = Stores}}} ->
                            {Ms, M1} = mstore:metrics(M),
                            Stores1 = gb_trees:enter(Bucket, M1, Stores),
                            S3 = S2#state{mstores = Stores1},
                            Ms1= btrie:fetch_keys_similar(Prefix, Ms),
                            {btrie:from_list(Ms1), S3};
                        _ ->
                            {btrie:new(), State}
                    end,
    {reply, {ok, MsR}, State1};

handle_call({write, Bucket, Metric, Time, Value}, _From, State) ->
    State1 = do_write(Bucket, Metric, Time, Value, State),
    {reply, ok, State1};

handle_call({read, Bucket, Metric, Time, Count, ReqID, Sender},
            _From, State = #state{async_read = Async}) ->
    State1 = maybe_async_read(Bucket, Metric, Time, Count, ReqID,
                              Sender, State#state{async_read = false}),
    State2 = State1#state{async_read = Async},
    {reply, ok, State2};

handle_call({read_rest, Bucket, Metric, Time, Count, Part, ReqID, Sender},
            _From, State = #state{async_read = Async}) ->
    State1 = maybe_async_read_rest(Bucket, Metric, Time, Count, Part, ReqID,
                                   Sender, State#state{async_read = false}),
    State2 = State1#state{async_read = Async},
    {reply, ok, State2};

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


handle_cast({write, Bucket, Metric, Time, Value}, State) ->
    State1 = do_write(Bucket, Metric, Time, Value, State),
    {noreply, State1};

handle_cast({get_bitmap, Bucket, Metric, Time, Ref, Sender}, State) ->
    {D, State1} = do_read_bitmap(Bucket, Metric, Time, State),
    Sender ! {reply, Ref, D},
    {noreply, State1};

handle_cast({read, Bucket, Metric, Time, Count, ReqID, Sender}, State) ->
    State1 = maybe_async_read(Bucket, Metric, Time, Count, ReqID,
                              Sender, State),
    {noreply, State1};
handle_cast({read_rest, Bucket, Metric, Time, Count, Part, ReqID, Sender},
            State) ->
    State1 = maybe_async_read_rest(Bucket, Metric, Time, Count, Part, ReqID,
                                   Sender, State),
    {noreply, State1};

handle_cast(update_env, State) ->
    {noreply, do_update_env(State)};

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
handle_info({'EXIT', _From, _Reason}, State = #state{mstores = MStore}) ->
    gb_trees:map(fun(_, {_, MSet}) ->
                         mstore:close(MSet)
                 end, MStore),
    {stop, normal, State#state{mstores = gb_trees:empty()}};

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
terminate(_Reason, #state{mstores = MStore, reader_pool = Pool}) ->
    gb_trees:map(fun(_, {_, MSet}) ->
                         mstore:close(MSet)
                 end, MStore),
    riak_core_vnode_worker_pool:stop(Pool, normal),
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
-spec do_update_env(state()) -> state().

do_update_env(State) ->
    Compression = application:get_env(dalmatiner_db,
                                      metric_transport_compression, snappy),
    AsyncRead = application:get_env(metric_vnode, async_read, false),
    AsyncMinSize = application:get_env(metric_vnode,
                                       async_min_size, 1000),
    FoldSize = application:get_env(metric_vnode, handoff_chunk, 10*1024),
    State1 = State#state{
               async_min_size = AsyncMinSize,
               compression = Compression,
               fold_size = FoldSize,
               async_read = AsyncRead
              },
    maybe_restart_pool(State1).

-spec bucket_dir(binary(), non_neg_integer()) -> string().

bucket_dir(Bucket, Partition) ->
    DataDir = application:get_env(riak_core, platform_data_dir, "data"),
    PartitionDir = filename:join([DataDir, integer_to_list(Partition)]),
    BucketDir = filename:join([PartitionDir, binary_to_list(Bucket)]),
    file:make_dir(PartitionDir),
    file:make_dir(BucketDir),
    BucketDir.

-spec new_store(non_neg_integer(), binary()) ->
                       entry().
new_store(Partition, Bucket) when is_binary(Bucket) ->
    BucketDir = bucket_dir(Bucket, Partition),
    PointsPerFile = dalmatiner_opt:ppf(Bucket),
    MaxOpenFiles = application:get_env(metric_vnode, max_files, 2),
    lager:debug("[metric_io:~p] Opening ~s@~p",
                [Partition, Bucket, PointsPerFile]),
    {ok, MSet} = mstore:new(BucketDir, [{file_size, PointsPerFile},
                                        {max_files, MaxOpenFiles}]),
    {0, MSet}.

-spec get_set(binary(), state()) ->
                     {ok, {entry(), state()}} |
                     {error, not_found}.
get_set(Bucket, State=#state{mstores = Store}) ->
    case gb_trees:lookup(Bucket, Store) of
        {value, MSet} ->
            {ok, {MSet, State}};
        none ->
            get_closed_set(Bucket, State)
    end.

get_closed_set(Bucket, State=#state{closed_mstores = Store}) ->
    case gb_trees:lookup(Bucket, Store) of
        {value, MSet} ->
            {ok, {MSet, State}};
        none ->
            case bucket_exists(State#state.partition, Bucket) of
                true ->
                    R = new_store(State#state.partition, Bucket),
                    Store1 = gb_trees:insert(Bucket, R, Store),
                    {ok, {R, State#state{closed_mstores = Store1}}};
                _ ->
                    {error, not_found}
            end
    end.
-spec get_or_create_set(binary(), state()) ->
                               {entry(), state()}.
get_or_create_set(Bucket, State=#state{mstores = Store}) ->
    case get_set(Bucket, State) of
        {ok, R} ->
            R;
        {error, not_found} ->
            MSet = new_store(State#state.partition, Bucket),
            Store1 = gb_trees:insert(Bucket, MSet, Store),
            {MSet, State#state{mstores = Store1}}
    end.

bucket_exists(Partition, Bucket) ->
    DataDir = case application:get_env(riak_core, platform_data_dir) of
                  {ok, DD} ->
                      DD;
                  _ ->
                      "data"
              end,
    PartitionDir = filename:join([DataDir, integer_to_list(Partition)]),
    BucketDir = filename:join([PartitionDir, binary_to_list(Bucket)]),
    filelib:is_dir(BucketDir).


calc_empty(I) ->
    case gb_trees:next(I) of
        none ->
            true;
        {_, {_, MSet}, I2} ->
            {Metrics, _MSet} = mstore:metrics(MSet),
            btrie:size(Metrics) =:= 0 andalso calc_empty(I2)
    end.

-spec do_write(binary(), binary(), pos_integer(), binary(), state()) ->
                      state().
do_write(Bucket, Metric, Time, Value, State) ->
    {{_, MSet}, State1} = get_or_create_set(Bucket, State),
    MSet1 = ddb_histogram:timed_update(
              {mstore, write},
              mstore, put, [MSet, Metric, Time, Value]),
    LastWritten = erlang:system_time(),
    Store1 = gb_trees:enter(Bucket, {LastWritten, MSet1},
                            State1#state.mstores),
    State1#state{mstores = Store1}.

do_read_bitmap(Bucket, Metric, Time, State) ->
    case get_set(Bucket, State) of
        {ok, {{_LastWritten, MSet}, S2}} ->
            R = mstore:bitmap(MSet, Metric, Time),
            {R, S2};
        _ ->
            lager:warning("[IO] Unknown metric: ~p/~p", [Bucket, Metric]),
            {{error, not_found}, State}
    end.
-spec do_read(binary(), binary(), non_neg_integer(), pos_integer(), state()) ->
                     {bitstring(), state()}.
do_read(Bucket, Metric, Time, Count, State = #state{})
  when is_binary(Bucket), is_binary(Metric), is_integer(Count) ->
    case get_set(Bucket, State) of
        {ok, {{_LastWritten, MSet}, S2}} ->
            {ok, Data} = ddb_histogram:timed_update(
                           {mstore, read},
                           mstore, get, [MSet, Metric, Time, Count]),
            {Data, S2};
        _ ->
            lager:warning("[IO] Unknown metric: ~p/~p", [Bucket, Metric]),
            {mmath_bin:empty(Count), State}
    end.

list_buckets(#state{dir = PartitionDir}) ->
    file:list_dir(PartitionDir).

compress(Data, #state{compression = snappy}) ->
    {ok, Dc} = snappyer:compress(Data),
    Dc;
compress(Data, #state{compression = none}) ->
    Data.

%% We only read asyncronously when:
%% 1) read_async is enabled
%% 2) we read at least async_min_size entries
%%
%% async_min_size is meant to prevent many tiny reads to create a
%% insane ammount of processes where it becomes more exensive
%% to create and tear down then it is to do the reads themselfs.
maybe_async_read(Bucket, Metric, Time, Count, ReqID, Sender,
                 State = #state{async_read = true, reader_pool = Pool,
                                node = N, partition = P,
                                async_min_size = MinSize})
  when Count >= MinSize,
       Pool =/= undefined ->
    case get_set(Bucket, State) of
        {ok, {{_LastWritten, MSet}, State1}} ->
            Work = #read_req{
                      mstore      = mstore:clone(MSet),
                      metric      = Metric,
                      time        = Time,
                      count       = Count,
                      compression = State#state.compression,
                      req_id      = ReqID
                     },
            riak_core_vnode_worker_pool:handle_work(Pool, Work, Sender),
            State1;
        _ ->
            lager:warning("[IO] Unknown metric: ~p/~p", [Bucket, Metric]),
            D = mmath_bin:empty(Count),
            Dc = compress(D, State),
            riak_core_vnode:reply(Sender, {ok, ReqID, {P, N}, Dc}),
            State
    end;
maybe_async_read(Bucket, Metric, Time, Count, ReqID, Sender,
                 State = #state{node = N, partition = P}) ->
    {D, State1} = do_read(Bucket, Metric, Time, Count, State),
    Dc = compress(D, State),
    riak_core_vnode:reply(Sender, {ok, ReqID, {P, N}, Dc}),
    State1.

maybe_async_read_rest(Bucket, Metric, Time, Count, Part, ReqID, Sender,
                      State = #state{node = N, partition = P, async_read = true,
                                     reader_pool = Pool,
                                     async_min_size = MinSize})
  when Count >= MinSize,
       Pool =/= undefined ->
    {ReadTime, ReadCount, MergeFn} = read_rest_prepare_part(Time, Count, Part),
    case get_set(Bucket, State) of
        {ok, {{_LastWritten, MSet}, State1}} ->
            Work = #read_req{
                      mstore      = mstore:clone(MSet),
                      metric      = Metric,
                      time        = ReadTime,
                      count       = ReadCount,
                      map_fn      = MergeFn,
                      compression = State#state.compression,
                      req_id      = ReqID
                     },
            riak_core_vnode_worker_pool:handle_work(Pool, Work, Sender),
            State1;
        _ ->
            D = mmath_bin:empty(Count),
            Data = MergeFn(D),
            Dc = compress(Data, State),
            riak_core_vnode:reply(Sender, {ok, ReqID, {P, N}, Dc}),
            State
    end;

maybe_async_read_rest(Bucket, Metric, Time, Count, Part, ReqID, Sender,
                      State = #state{node = N, partition = P}) ->
    {ReadTime, ReadCount, MergeFn} = read_rest_prepare_part(Time, Count, Part),
    {D, State1} = do_read(Bucket, Metric, ReadTime, ReadCount, State),
    Data = MergeFn(D),
    Dc = compress(Data, State),
    riak_core_vnode:reply(Sender, {ok, ReqID, {P, N}, Dc}),
    State1.

read_rest_prepare_part(Time, Count, Part) ->
    case Part of
        {Offset, Len, Bin} when Offset =:= 0 ->
            ReadTime = Time + Len,
            ReadCount = Count - Len,
            MergeFn = fun(ReadData) ->
                              <<Bin/binary, ReadData/binary>>
                      end,
            {ReadTime, ReadCount, MergeFn};
        {Offset, Len, Bin} when Offset + Len =:= Count ->
            ReadTime = Time,
            ReadCount = Count - Len,
            MergeFn = fun(ReadData) ->
                              <<ReadData/binary, Bin/binary>>
                      end,
            {ReadTime, ReadCount, MergeFn};
        {Offset, Len, Bin} ->
            ReadTime = Time,
            ReadCount = Count,
            MergeFn = fun(ReadData) ->
                              S = ?DATA_SIZE,
                              D1 = binary_part(ReadData, 0, Offset * S),
                              D2 = binary_part(ReadData, Count * S,
                                               (Offset + Len - Count) * S),
                              <<D1/binary, Bin/binary, D2/binary>>
                      end,
            {ReadTime, ReadCount, MergeFn}
    end.

maybe_start_pool(S = #state{partition = VNodeIndex,
                            reader_pool = undefined,
                            async_read = true,
                            pool_config = {PoolSize, Strategy}})
  when PoolSize > 0 ->
    WorkerMod = metric_io_worker,
    WorkerArgs = [],
    WorkerProps = [],
    PoolOpts = [{strategy, Strategy}],
    {ok, Pool} = riak_core_vnode_worker_pool:start_link(
                   WorkerMod, PoolSize, VNodeIndex, WorkerArgs, WorkerProps,
                   PoolOpts),
    S#state{reader_pool = Pool};

maybe_start_pool(State) ->
    State.

get_pool_config() ->
    Strategy = application:get_env(metric_vnode, queue_strategy, fifo),
    PoolSize = application:get_env(metric_vnode, io_queue_size, 5),
    {PoolSize, Strategy}.

maybe_restart_pool(State = #state{async_read = false,
                                  reader_pool = undefined}) ->
    State;
maybe_restart_pool(State = #state{async_read = false, reader_pool = Pid}) ->
    lager:warning("Shutting down IO pool due to config update"),
    riak_core_vnode_worker_pool:shutdown_pool(Pid, 5000),
    State#state{reader_pool = undefined};
maybe_restart_pool(State = #state{pool_config = Conf}) ->
    case get_pool_config() of
        Conf1 when Conf1 =:= Conf ->
            maybe_restart_pool(State);
        Conf1 ->
            case State#state.reader_pool of
                undefined ->
                    ok;
                Pid ->
                    lager:warning("Reastarting IO pool due to config update"),
                    riak_core_vnode_worker_pool:shutdown_pool(Pid, 5000)
            end,
            State1 = State#state{pool_config = Conf1, reader_pool = undefined},
            maybe_start_pool(State1)
    end.

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

%% API
-export([start_link/1,
         empty/1, fold/3, delete/1, delete/2, delete/3, close/1,
         buckets/1, metrics/2, metrics/3,
         read/7, write/5, write/6]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(WEEK, 604800). %% Seconds in a week.
-define(MAX_Q_LEN, 20).

-record(state, {
          partition,
          node,
          mstore=gb_trees:empty(),
          dir,
          fold_size
         }).

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
start_link(Partition) ->
    gen_server:start_link(?MODULE, [Partition], []).

write(Pid, Bucket, Metric, Time, Value) ->
    write(Pid, Bucket, Metric, Time, Value, ?MAX_Q_LEN).

write(Pid, Bucket, Metric, Time, Value, MaxLen) ->
    {message_queue_len, Len} = erlang:process_info(Pid, message_queue_len),
    dalmatiner_metrics:inc(Len, ioq),

    if
        Len > MaxLen ->
            swrite(Pid, Bucket, Metric, Time, Value);
        true ->
            gen_server:cast(Pid, {write, Bucket, Metric, Time, Value})
    end.

swrite(Pid, Bucket, Metric, Time, Value) ->
    gen_server:call(Pid, {write, Bucket, Metric, Time, Value}).

read(Pid, Bucket, Metric, Time, Count, ReqID, Sender) ->
    gen_server:cast(Pid, {read, Bucket, Metric, Time, Count, ReqID, Sender}).

buckets(Pid) ->
    gen_server:call(Pid, buckets).

metrics(Pid, Bucket) ->
    gen_server:call(Pid, {metrics, Bucket}).

metrics(Pid, Bucket, Prefix) ->
    gen_server:call(Pid, {metrics, Bucket, Prefix}).

fold(Pid, Fun, Acc0) ->
    gen_server:call(Pid, {fold, Fun, Acc0}).

empty(Pid) ->
    gen_server:call(Pid, empty).

delete(Pid) ->
    gen_server:call(Pid, delete).

close(Pid) ->
    gen_server:call(Pid, close).

delete(Pid, Bucket) ->
    gen_server:call(Pid, {delete, Bucket}).

delete(Pid, Bucket, Before) ->
    gen_server:call(Pid, {delete, Bucket, Before}).

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
    DataDir = case application:get_env(riak_core, platform_data_dir) of
                  {ok, DD} ->
                      DD;
                  _ ->
                      "data"
              end,
    FoldSize = case application:get_env(metric_vnode, handoff_chunk) of
                   {ok, FS} ->
                       FS;
                   _ ->
                       10*1024
               end,
    PartitionDir = [DataDir, $/,  integer_to_list(Partition)],

    {ok, #state{ partition = Partition,
                 node = node(),
                 dir = PartitionDir,
                 fold_size = FoldSize
               }}.

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
          max_delta = 300,
          fold_size = 82800
        }).


fold_fun(Metric, Time, V,
         Acc =
             #facc{metric = Metric2,
                   lacc = []}) when
      Metric =/= Metric2 ->
    Size = mmath_bin:length(V),
    Acc#facc{
      metric = Metric,
      last = Time + Size,
      size = Size,
      lacc = [{Time, V}]};
fold_fun(Metric, Time, V,
         Acc =
             #facc{metric = Metric2,
                   bucket = Bucket,
                   lacc = AccL,
                   acc_fun = Fun,
                   hacc = AccIn}) when
      Metric =/= Metric2 ->
    Size = mmath_bin:length(V),
    AccOut = Fun({Bucket, Metric2}, lists:reverse(AccL), AccIn),
    Acc#facc{
      metric = Metric,
      last = Time + Size,
      size = Size,
      hacc = AccOut,
      lacc = [{Time, V}]};

fold_fun(Metric, Time, V,
         Acc =
             #facc{metric = Metric,
                   bucket = Bucket,
                   size = _Size,
                   lacc = AccL,
                   acc_fun = Fun,
                   hacc = AccIn,
                   fold_size = _FoldSize}) when
      _Size > _FoldSize ->
    AccOut = Fun({Bucket, Metric}, lists:reverse(AccL), AccIn),
    Size = mmath_bin:length(V),
    Acc#facc{
      size = Size,
      hacc = AccOut,
      last = Time + Size,
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
    {ok, MStore} = mstore:open(BucketDir),
    Acc1 = #facc{hacc = AccIn,
                 bucket = Bucket,
                 acc_fun = Fun},
    AccOut = mstore:fold(MStore, fun fold_fun/4, Acc1),
    mstore:close(MStore),
    case AccOut of
        #facc{lacc=[], hacc=HAcc} ->
            {HAcc, Fun};
        #facc{bucket = Bucket, metric = Metric,
              lacc=AccL, hacc=HAcc}->
            {Fun({Bucket, Metric}, lists:reverse(AccL), HAcc), Fun}
    end.

fold_byckets_fun(PartitionDir, Buckets, Fun, Acc0) ->
    Buckets1 = [{[PartitionDir, $/, BucketS], list_to_binary(BucketS)}
                || BucketS <- Buckets],
    fun() ->
            {Out, _} = lists:foldl(fun bucket_fold_fun/2, {Acc0, Fun},
                                   Buckets1),
            Out
    end.

handle_call({fold, Fun, Acc0}, _From,
            State = #state{partition = Partition}) ->
    DataDir = application:get_env(riak_core, platform_data_dir, "data"),
    PartitionDir = [DataDir, $/,  integer_to_list(Partition)],
    case file:list_dir(PartitionDir) of
        {ok, Buckets} ->
            AsyncWork = fold_byckets_fun(PartitionDir, Buckets, Fun, Acc0),
            {reply, {ok, AsyncWork}, State};
        _ ->
            {reply, empty, State}
    end;

handle_call(empty, _From, State) ->
    R = calc_empty(gb_trees:iterator(State#state.mstore)),
    {reply, R, State};

handle_call(delete, _From, State = #state{partition = Partition}) ->
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
    {reply, ok, State#state{mstore=gb_trees:empty()}};

handle_call(close, _From, State) ->
    gb_trees:map(fun(_, {_, MSet}) ->
                         mstore:close(MSet)
                 end, State#state.mstore),
    State1 = State#state{mstore=gb_trees:empty()},
    {reply, ok, State1};
%%{stop, normal, State1};

handle_call({delete, Bucket}, _From,
            State = #state{dir = Dir}) ->
    {R, State1} = case get_set(Bucket, State) of
                      {ok, {{_, MSet}, S1}} ->
                          mstore:delete(MSet),
                          file:del_dir([Dir, $/, Bucket]),
                          MStore = gb_trees:delete(Bucket, S1#state.mstore),
                          {ok, S1#state{mstore = MStore}};
                      _ ->
                          {not_found, State}
                  end,
    {reply, R, State1};

handle_call({delete, Bucket, Before}, _From, State) ->
    {R, State1} = case get_set(Bucket, State) of
                      {ok, {{Res, MSet}, S1}} ->
                          {ok, MSet1} = mstore:delete(MSet, Before),
                          V = {Res, MSet1},
                          MStore = gb_trees:enter(Bucket, V, S1#state.mstore),
                          {ok, S1#state{mstore = MStore}};
                      _ ->
                          {not_found, State}
                  end,
    {reply, R, State1};

handle_call(buckets, _From, State = #state{partition = P}) ->
    DataDir = case application:get_env(riak_core, platform_data_dir) of
                  {ok, DD} ->
                      DD;
                  _ ->
                      "data"
              end,
    PartitionDir = [DataDir, $/,  integer_to_list(P)],
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
                       {ok, {{_, M}, S2}} ->
                           {mstore:metrics(M), S2};
                       _ ->
                           {btrie:new(), State}
                   end,
    {reply, {ok, Ms}, State1};

handle_call({metrics, Bucket, Prefix}, _From, State) ->
    {MsR, State1} = case get_set(Bucket, State) of
                        {ok, {{_, M}, S2}} ->
                            Ms = mstore:metrics(M),
                            Ms1 = btrie:fetch_keys_similar(Prefix, Ms),
                            {btrie:from_list(Ms1), S2};
                        _ ->
                            {btrie:new(), State}
                    end,
    {reply, {ok, MsR}, State1};

handle_call({write, Bucket, Metric, Time, Value}, _From, State) ->
    State1 = do_write(Bucket, Metric, Time, Value, State),
    {reply, ok, State1};

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

handle_cast({read, Bucket, Metric, Time, Count, ReqID, Sender},
            State = #state{node = N, partition = P}) ->
    {D, State1} =
        case get_set(Bucket, State) of
            {ok, {{Resolution, MSet}, S2}} ->
                {ok, Data} = mstore:get(MSet, Metric, Time, Count),
                {{Resolution, Data}, S2};
            _ ->
                lager:warning("[IO] Unknown metric: ~p/~p", [Bucket, Metric]),
                Resolution = get_resolution(Bucket),
                {{Resolution, mmath_bin:empty(Count)}, State}
        end,
    riak_core_vnode:reply(Sender, {ok, ReqID, {P, N}, D}),
    {noreply, State1};

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
handle_info({'EXIT', _From, _Reason}, State = #state{mstore = MStore}) ->
    gb_trees:map(fun(_, {_, MSet}) ->
                         mstore:close(MSet)
                 end, MStore),
    {stop, normal, State#state{mstore=gb_trees:empty()}};

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
terminate(_Reason, #state{mstore = MStore}) ->
    gb_trees:map(fun(_, {_, MSet}) ->
                         mstore:close(MSet)
                 end, MStore),
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

-spec ppf(binary()) -> pos_integer().

ppf(Bucket) ->
    case dalmatiner_opt:get(<<"buckets">>, Bucket,
                            <<"points_per_file">>,
                            {metric_vnode, points_per_file}, ?WEEK) of
        PPF when is_integer(PPF), PPF > 0 ->
            PPF;
        _ ->
            ?WEEK
    end.

-spec bucket_dir(binary(), non_neg_integer()) -> string().

bucket_dir(Bucket, Partition) ->
    DataDir = application:get_env(riak_core, platform_data_dir, "data"),
    PartitionDir = DataDir ++ [$/, integer_to_list(Partition)],
    BucketDir = PartitionDir ++ [$/, binary_to_list(Bucket)],
    file:make_dir(PartitionDir),
    file:make_dir(BucketDir),
    BucketDir.

new_store(Partition, Bucket) when is_binary(Bucket) ->
    BucketDir = bucket_dir(Bucket, Partition),
    PointsPerFile = ppf(Bucket),
    Resolution = get_resolution(Bucket),
    {ok, MSet} = mstore:new(BucketDir, [{file_size, PointsPerFile}]),
    {Resolution, MSet}.



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


calc_empty(I) ->
    case gb_trees:next(I) of
        none ->
            true;
        {_, {_, MSet}, I2} ->
            btrie:size(mstore:metrics(MSet)) =:= 0
                andalso calc_empty(I2)
    end.

do_write(Bucket, Metric, Time, Value, State) ->
    {{R, MSet}, State1} = get_or_create_set(Bucket, State),
    MSet1 = mstore:put(MSet, Metric, Time, Value),
    Store1 = gb_trees:update(Bucket, {R, MSet1}, State1#state.mstore),
    State1#state{mstore=Store1}.

get_resolution(Bucket) ->
    dalmatiner_opt:get(
      <<"buckets">>, Bucket, <<"resolution">>,
      {metric_vnode, resolution}, 1000).

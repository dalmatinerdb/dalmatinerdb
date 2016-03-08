%% @doc Interface for dalmatiner-admin commands.
-module(dalmatiner_console).
-export([
         ttl/1,
         buckets/1,
         status/1,
         repair/0,
         repair/1
        ]).

-ignore_xref([
              ttl/1,
              status/1,
              repair/0,
              repair/1,
              buckets/1
             ]).

-define(WEEK, 604800). %% Seconds in a week.

-type bucket() :: nonempty_string().
-type bucket_dir() :: nonempty_string().
-type partition() :: nonempty_string().

buckets([]) ->
    {ok, Bkts} = metric:list(),
    [io:format("~s~n", [B]) || B <- Bkts],
    ok.

ttl([Buckets]) ->
    Bucket = list_to_binary(Buckets),
    case dalmatiner_opt:lifetime(Bucket) of
        TTL when is_integer(TTL) ->
            Res = dalmatiner_opt:resolution(Bucket),
            TTLs = cuttlefish_datatypes:to_string(TTL * Res, {duration, ms}),
            io:format("~s~n", [TTLs]);
        TTL ->
            io:format("~p~n", [TTL])
    end;

ttl([Buckets, "infinity"]) ->
    Bucket = list_to_binary(Buckets),
    metric:update_ttl(Bucket, infinity);


ttl([Buckets, TTLs]) ->
    Bucket = list_to_binary(Buckets),
    TTL = try
              integer_to_list(TTLs)
          catch
              _:_ ->
                  TTLms = cuttlefish_datatypes:from_string(
                            TTLs, {duration, ms}),
                  Res = dalmatiner_opt:resolution(Bucket),
                  TTLms div Res
          end,
    metric:update_ttl(Bucket, TTL).

-spec(status([]) -> ok).
status([]) ->
    try
        Stats = dalmatiner_metrics:statistics(),
        StatString = format_stats(Stats,
                            ["-------------------------------------------\n",
                            io_lib:format("1-minute stats for ~p~n",[node()])]),
        io:format("~s\n", [StatString])
    catch
        Exception:Reason ->
            lager:error("Status failed ~p:~p", [Exception,
                    Reason]),
            io:format("Status failed, see log for details~n"),
            error
    end.

format_stats([], Acc) ->
    lists:reverse(Acc);
format_stats([{Stat, V}|T], Acc) ->
    format_stats(T, [io_lib:format("~s : ~p~n",
                                   [format_stat_key(Stat), V])|Acc]).

format_stat_key([]) ->
    [];
format_stat_key([[]|T]) ->
    format_stat_key(T);
format_stat_key([Key|T]) when is_list(Key) ->
    format_stat_key(Key ++ T);
format_stat_key([Key]) ->
    Key;
format_stat_key([Key|T]) ->
    [Key, <<".">>, format_stat_key(T)].

-spec repair() -> ok.
repair() ->
    DataDir = application:get_env(riak_core, platform_data_dir, "data"),
    {ok, Dirs} = file:list_dir(DataDir),
    Partitions = [P || P <- Dirs, is_partition(P)],
    NumRepairs = lists:sum([repair(P) || P <- Partitions]),
    io:format("Total repairs across ~p partitions: ~p~n", [length(Partitions),
                                                            NumRepairs]).

-spec repair(partition()) -> integer().
repair(Partition) when is_list(Partition) ->
    BucketDirs = bucket_dirs(Partition),
    TaintedStores = [B || B <- BucketDirs, integrity_check(B) =:= false],
    UntaintedStores = ordsets:subtract(ordsets:from_list(BucketDirs),
                                       ordsets:from_list(TaintedStores)),

    NumRepairs = length(TaintedStores),
    io:format("~p repairs for partition ~p~n", [NumRepairs, Partition]),
    [repair_store(B) || B <- TaintedStores],
    [reindex_store(B) || B <- UntaintedStores],
    NumRepairs.

-spec integrity_check({bucket(), bucket_dir()}) -> boolean().
integrity_check({_, Dir}) ->
    case mstore:open(Dir) of
        {ok, _Mstore} ->
            true;
        _E ->
            %% io:format("Opening mstore ~p failed ~p~n", [Dir, E]),
            false
    end.

-spec repair_store({bucket(), bucket_dir()}) -> ok.
repair_store({Bucket, Dir}) ->
    Opts = [{file_size, ppf(Bucket)}],
    {ok, Mstore} = mstore:new(Dir, Opts),
    mstore:close(Mstore).

-spec reindex_store({bucket(), bucket_dir()}) -> ok.
reindex_store({_, Dir}) ->
    {ok, Mstore} = mstore:open(Dir),
    {ok, Mstore1} = mstore:reindex(Mstore),
    mstore:close(Mstore1).

-spec ppf(bucket()) -> pos_integer().
ppf(Bucket) ->
    case dalmatiner_opt:get(<<"buckets">>, Bucket,
                            <<"points_per_file">>,
                            {metric_vnode, points_per_file}, ?WEEK) of
        PPF when is_integer(PPF), PPF > 0 ->
            PPF;
        _ ->
            ?WEEK
    end.

-spec is_partition(partition()) -> boolean().
is_partition(Partition) when is_list(Partition) ->
   is_number(catch(erlang:list_to_integer(Partition))).

-spec bucket_dirs(partition()) -> [{bucket(), bucket_dir()}].
bucket_dirs(Partition) ->
    DataDir = application:get_env(riak_core, platform_data_dir, "data"),
    PartitionDir = [DataDir, $/, Partition],
    {ok, Buckets} = file:list_dir(PartitionDir),
    BucketDir = fun(B) -> [PartitionDir, $/, B] end,
    [{B, lists:flatten(BucketDir(B))} || B <- Buckets].

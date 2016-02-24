%% @doc Interface for dalmatiner-admin commands.
-module(dalmatiner_console).
-export([
         ttl/1,
         buckets/1,
         status/1
        ]).

-ignore_xref([
              ttl/1,
              buckets/1
             ]).

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
    format_stats(T, [io_lib:format("~s : ~p~n", [format_stat_key(Stat), V])|Acc]).

%format_stat_key(Stat) ->
%    format_stat_key(Stat, []).

format_stat_key([]) ->
    [];
format_stat_key([Part]) ->
    Part;
format_stat_key([[]|T]) ->
    format_stat_key(T);
format_stat_key([Key|T]) when is_list(Key) ->
    case format_stat_key(Key) of
        [] -> format_stat_key(T);
        Part -> [Part, <<".">>, format_stat_key(T)]
    end;
format_stat_key([Key|T]) ->
    [Key, <<".">>, format_stat_key(T)].

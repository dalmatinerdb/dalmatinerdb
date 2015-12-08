%% @doc Interface for dalmatiner-admin commands.
-module(dalmatiner_console).
-export([
         ttl/1,
         buckets/1
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

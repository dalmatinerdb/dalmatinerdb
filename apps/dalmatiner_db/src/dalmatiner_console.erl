%% @doc Interface for dalmatiner-admin commands.
-module(dalmatiner_console).
-export([
         ttl/1,
         buckets/1,
         create/1
        ]).

-ignore_xref([
              ttl/1,
              buckets/1,
              create/1
             ]).

create([BucketS, ResS, PPFS, TTLS]) ->
    case create([BucketS, ResS, PPFS]) of
        ok ->
            ttl([BucketS, TTLS]);
        E ->
            E
    end;

create([BucketS, ResS, PPFS]) ->
    Bucket = list_to_binary(BucketS),
    Res = cuttlefish_datatypes:from_string(
            ResS, {duration, ms}),
    dalmatiner_opt:set_resolution(Bucket, Res),
    PPF = cuttlefish_datatypes:from_string(
            PPFS, {duration, ms}) div Res,
    dalmatiner_opt:set_ppf(Bucket, PPF),
    io:format("~s ~s ~s~n",
              [Bucket,
               format_time(Res, ms),
               format_time(PPF * Res, ms)]).

buckets([]) ->
    Bs1 = riak_core_metadata:to_list({<<"buckets">>, <<"resolution">>}),
    Bkts = [B || {B, _} <- Bs1],
    io:format("~30s | ~-15s | ~-15s | ~-15s~n",
              ["Bucket", "Resolution", "File Size", "TTL"]),
    io:format("~30c-+-~-15c-+-~-15c-+-~-15c~n",
              [$-, $-, $-, $-]),

    print_buckets(Bkts).

print_buckets([]) ->
    ok;
print_buckets([Bucket | R]) ->
    case metric:bucket_info(Bucket) of
        {ok, {Res, PPF, infinity}} ->
            io:format("~30s | ~-15s | ~-15s | ~-15s~n",
                      [Bucket,
                       format_time(Res, ms),
                       format_time(PPF * Res, ms),
                       inf]);
        {ok, {Res, PPF, TTL}} ->
            io:format("~30s | ~-15s | ~-15s | ~-15s~n",
                      [Bucket,
                       format_time(Res, ms),
                       format_time(PPF * Res, ms),
                       format_time(TTL * Res, ms)])
    end,
    print_buckets(R).

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

ttl([BucketS, "infinity"]) ->
    Bucket = list_to_binary(BucketS),
    metric:update_ttl(Bucket, infinity);


ttl([BucketS, TTLs]) ->
    Bucket = list_to_binary(BucketS),
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

format_time(T, ms)
  when T >= 1000
       andalso T rem 1000 =:= 0 ->
    format_time(T div 1000, s);

format_time(T, s)
  when T >= 60
       andalso T rem 60 =:= 0 ->
    format_time(T div 60, m);

format_time(T, m)
  when T >= 60
       andalso T rem 60 =:= 0 ->
    format_time(T div 60, h);

format_time(T, h)
  when T >= 24
       andalso T rem 24 =:= 0 ->
    format_time(T div 24, d);

format_time(T, d)
  when T >= 7
       andalso T rem 7 =:= 0 ->
    format_time(T div 7, w);
format_time(T, F) ->
    io_lib:format("~b~s", [T, F]).

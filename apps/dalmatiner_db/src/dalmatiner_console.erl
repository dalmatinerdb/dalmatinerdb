%% @doc Interface for dalmatiner-admin commands.
-module(dalmatiner_console).
-export([
         delete/1,
         ttl/1,
         buckets/1,
         status/1,
         create/1
        ]).

-ignore_xref([
              delete/1,
              ttl/1,
              buckets/1,
              status/1,
              create/1
             ]).

delete([BucketS]) ->
    Bucket = list_to_binary(BucketS),
    event:delete(Bucket),
    metric:delete(Bucket),
    dalmatiner_opt:delete(Bucket).

create([BucketS, ResS, PPFS, TTLS]) ->
    ok = create([BucketS, ResS, PPFS]),
    ttl([BucketS, TTLS]);

create([BucketS, ResS, PPFS, TTLS, GraceS]) ->
    ok = create([BucketS, ResS, PPFS, TTLS]),
    Bucket = list_to_binary(BucketS),
    Grace = decode_time(GraceS, ns),
    io:format("Setting grace to: ~s~n", [format_time(Grace, ns)]),
    dalmatiner_opt:set_grace(Bucket, Grace);

create([BucketS, ResS, PPFS]) ->
    Bucket = list_to_binary(BucketS),
    Res = cuttlefish_datatypes:from_string(
            ResS, {duration, ms}),
    dalmatiner_opt:set_resolution(Bucket, Res),
    PPF = cuttlefish_datatypes:from_string(
            PPFS, {duration, ms}) div Res,
    dalmatiner_opt:set_ppf(Bucket, PPF),
    io:format("Carated ~s with a resolution of ~s and ~s worth of points "
              "per file.~n",
              [Bucket,
               format_time(Res, ms),
               format_time(PPF * Res, ms)]).

buckets([]) ->
    Bkts = dalmatiner_bucket:list(),
    io:format("~30s | ~-15s | ~-15s | ~-15s | ~-15s~n",
              ["Bucket", "Resolution", "File Size", "Grace", "TTL"]),
    io:format("~30c-+-~-15c-+-~-15c-+-~-15c-+-~-15c~n",
              [$-, $-, $-, $-, $-]),

    print_buckets(Bkts).

print_buckets([]) ->
    ok;
print_buckets([Bucket | R]) ->
    case dalmatiner_bucket:info(Bucket) of
        #{
           resolution := Res,
           ppf := PPF,
           grace := Grace,
           ttl := infinity
         } ->
            io:format("~30s | ~-15s | ~-15s | ~-15s | ~-15s~n",
                      [Bucket,
                       format_time(Res, ms),
                       format_time(PPF * Res, ms),
                       format_time(Grace, ns),
                       infinity]);
        #{
           resolution := Res,
           ppf := PPF,
           grace := Grace,
           ttl := TTL
         } ->
            io:format("~30s | ~-15s | ~-15s | ~-15s | ~-15s~n",
                      [Bucket,
                       format_time(Res, ms),
                       format_time(PPF * Res, ms),
                       format_time(Grace, ns),
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

ttl([BucketS, "inf"]) ->
    Bucket = list_to_binary(BucketS),
    io:format("TTL set to: ~s~n", [infinity]),
    metric:update_ttl(Bucket, infinity),
    ok;
ttl([BucketS, "infinity"]) ->
    Bucket = list_to_binary(BucketS),
    io:format("TTL set to: ~s~n", [infinity]),
    metric:update_ttl(Bucket, infinity),
    ok;

ttl([BucketS, TTLs]) ->
    Bucket = list_to_binary(BucketS),
    Res = dalmatiner_opt:resolution(Bucket),
    TTL = try
              integer_to_list(TTLs)
          catch
              _:_ ->
                  TTLms = cuttlefish_datatypes:from_string(
                            TTLs, {duration, ms}),
                  TTLms div Res
          end,
    io:format("TTL set to: ~s~n", [format_time(TTL*Res, ms)]),
    metric:update_ttl(Bucket, TTL),
    ok.

-spec(status([]) -> ok).
status([]) ->
    try
        Stats = dalmatiner_metrics:statistics(),
        StatString = format_stats(Stats,
                            ["-------------------------------------------\n",
                            io_lib:format("1-minute stats for ~p~n",
                                          [node()])]),
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

format_time(T, ns)
  when T >= 1000
       andalso T rem 1000 =:= 0 ->
    format_time(T div 1000, us);

format_time(T, us)
  when T >= 1000
       andalso T rem 1000 =:= 0 ->
    format_time(T div 1000, ms);

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

decode_time(TimeS, Unit) ->
    try
        integer_to_list(TimeS)
    catch
        _:_ ->
            decode_time_(TimeS, Unit)
    end.


decode_time_(TimeS, ns) ->
    decode_time_(TimeS, us) * 1000;
decode_time_(TimeS, us) ->
    decode_time_(TimeS, ms) * 1000;
decode_time_(TimeS, Unit) ->
    cuttlefish_datatypes:from_string(
      TimeS, {duration,  Unit}).

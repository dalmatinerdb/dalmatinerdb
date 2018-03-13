-module(dalmatiner_tcp).

-behaviour(ranch_protocol).

-include_lib("dproto/include/dproto.hrl").
-include_lib("mmath/include/mmath.hrl").

-export([init/4, start_link/4]).

-record(state,
        {n = 1 :: pos_integer(),
         w = 1 :: pos_integer(),
         rr_min_time = 0 :: non_neg_integer()
        }).

-record(sstate,
        {
          recv_size = 0,
          max_bkt_batch_size = 500,
          last = undefined :: non_neg_integer() | undefined,
          max_diff = 1 :: pos_integer(),
          dict, % :: bkt_dict:bkt_dict(),
          hpts = false :: boolean()
        }).


-type state() :: #state{}.

-type stream_state() :: #sstate{}.

start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

init(Ref, Socket, Transport, _Opts = []) ->
    {ok, N} = application:get_env(dalmatiner_db, n),
    {ok, W} = application:get_env(dalmatiner_db, w),
    RRTime = application:get_env(dalmatiner_db, read_repair_min_time, 0),
    State = #state{n = N, w = W, rr_min_time = RRTime},
    ok = Transport:setopts(Socket, [{packet, 4}]),
    ok = ranch:accept_ack(Ref),
    loop(Socket, Transport, State).

-spec loop(port(), term(), state()) -> ok.

loop(Socket, Transport, State) ->
    case Transport:recv(Socket, 0, 5000) of
        {ok, Data} ->
            {ot, TIDs, Data1} = dproto_tcp:decode_ot(Data),
            S = case TIDs of
                    undefined ->
                        undefined;
                    {TraceID, ParentID} ->
                        otters:start(ddb, TraceID, ParentID)
                end,
            S1 = otters:log(S, "package received"),
            case dproto_tcp:decode(Data1) of
                buckets ->
                    S2 = otters:log(S1, "decoded"),
                    Bs = dalmatiner_bucket:list(),
                    Transport:send(Socket, dproto_tcp:encode_metrics(Bs)),
                    otters:finish(S2),
                    loop(Socket, Transport, State);
                {list, Bucket} ->
                    true = dalmatiner_opt:bucket_exists(Bucket),
                    S2 = otters:log(S1, "decoded"),
                    {ok, Ms} = metric:list(Bucket),
                    Transport:send(Socket, dproto_tcp:encode_metrics(Ms)),
                    otters:finish(S2),
                    loop(Socket, Transport, State);
                {delete, Bucket} ->
                    true = dalmatiner_opt:bucket_exists(Bucket),
                    S2 = otters:log(S1, "decoded"),
                    metric:delete(Bucket),
                    otters:finish(S2),
                    loop(Socket, Transport, State);
                {list, Bucket, Prefix} ->
                    true = dalmatiner_opt:bucket_exists(Bucket),
                    S2 = otters:log(S1, "decoded"),
                    {ok, Ms} = metric:list(Bucket, Prefix),
                    Transport:send(Socket, dproto_tcp:encode_metrics(Ms)),
                    otters:finish(S2),
                    loop(Socket, Transport, State);
                {info, Bucket} ->
                    S2 = otters:log(S1, "decoded"),
                    {ok, Info} = dalmatiner_bucket:info(Bucket),
                    InfoBin = dproto_tcp:encode_bucket_info(Info),
                    Transport:send(Socket, InfoBin),
                    otters:finish(S2),
                    loop(Socket, Transport, State);
                {get, B, M, T, C, Opts} ->
                    true = dalmatiner_opt:bucket_exists(B),
                    S2 = otters:log(S1, "decoded"),
                    Opts1 = apply_rtt(State#state.rr_min_time, B, T, C, Opts),
                    S3 = do_send(Socket, Transport, B, M, T, C, Opts1, S2),
                    otters:finish(S3),
                    loop(Socket, Transport, State);
                {ttl, Bucket, TTL} ->
                    S2 = otters:log(S1, "decoded"),
                    ok = metric:update_ttl(Bucket, TTL),
                    otters:finish(S2),
                    loop(Socket, Transport, State);
                {events, Bucket, Es} ->
                    true = dalmatiner_opt:bucket_exists(Bucket),
                    S2 = otters:log(S1, "decoded"),
                    event:append(Bucket, Es),
                    otters:finish(S2),
                    loop(Socket, Transport, State);
                {get_events, Bucket, Start, End} ->
                    true = dalmatiner_opt:bucket_exists(Bucket),
                    S2 = otters:log(S1, "decoded"),
                    Size = event:split(Bucket),
                    Splits = estore:make_splits(Start, End, Size),
                    otters:finish(S2),
                    get_events(Bucket, [], Splits, Socket, Transport, State);
                {get_events, Bucket, Start, End, Filter} ->
                    true = dalmatiner_opt:bucket_exists(Bucket),
                    S2 = otters:log(S1, "decoded"),
                    Size = event:split(Bucket),
                    Splits = estore:make_splits(Start, End, Size),
                    otters:finish(S2),
                    get_events(Bucket, Filter, Splits, Socket, Transport,
                               State);
                {stream, Bucket, Delay} ->
                    true = dalmatiner_opt:bucket_exists(Bucket),
                    S2 = otters:log(S1, "decoded"),
                    lager:info("[tcp] Entering stream mode for bucket '~s' "
                               "and a max delay of: ~p", [Bucket, Delay]),
                    ok = Transport:setopts(Socket, [{packet, 0}]),
                    otters:finish(S2),
                    BDSize = application:get_env(
                               dalmatiner_db, max_bkt_batch_size, 500),
                    stream_loop(Socket, Transport,
                                #sstate{max_diff = Delay,
                                        max_bkt_batch_size = BDSize,
                                        dict = bkt_pdict:new(Bucket,
                                                             State#state.n,
                                                             State#state.w,
                                                             false)},
                                {incomplete, <<>>});
                {stream_v2, Bucket, Delay, HPTS} ->
                    true = dalmatiner_opt:bucket_exists(Bucket),
                    S2 = otters:log(S1, "decoded"),
                    lager:info("[tcp] Entering stream mode for bucket '~s' "
                               "and a max delay of: ~p HPTS: ~p",
                               [Bucket, Delay, HPTS]),
                    ok = Transport:setopts(Socket, [{packet, 0}]),
                    otters:finish(S2),
                    BDSize = application:get_env(
                               dalmatiner_db, max_bkt_batch_size, 500),
                    stream_loop(Socket, Transport,
                                #sstate{max_diff = Delay,
                                        hpts = HPTS,
                                        max_bkt_batch_size = BDSize,
                                        dict = bkt_pdict:new(Bucket,
                                                             State#state.n,
                                                             State#state.w,
                                                             HPTS)},
                                {incomplete, <<>>})
            end;
        {error, timeout} ->
            loop(Socket, Transport, State);
        {error, closed} ->
            ok;
        E ->
            lager:error("[tcp:loop] Error: ~p~n", [E]),
            ok = Transport:close(Socket)
    end.

%% When the get request has a read repair option other than the default, that
%% option will be honoured.  Otherwise, if the cutoff date (now - min RRT) is
%% larger then the first timestamp of the read, we have data that is older
%% and will perform a read repair.
apply_rtt(MinRRT, B, T, C, Opts) when is_list(Opts) ->
    RROpt = proplists:lookup(rr, Opts),
    Opts1 = proplists:delete(rr, Opts),
    [apply_rtt(MinRRT, B, T, C, RROpt) | Opts1];
apply_rtt(0, _B, _T, _C, RROpt) ->
    RROpt;
apply_rtt(MinRRT, B, First, Count, {rr, default} = RROpt) ->

    PartialRepairs = application:get_env(dalmatiner_db, partial_rr, false),
    %% The bucket Resolultion
    Res = dalmatiner_opt:resolution(B),
    %% Current time
    Now = erlang:system_time(milli_seconds),

    %% We scale the current time to the bucket resoltuion
    NowScaled = Now div Res,
    %% We scale the minimum Read repair time to the current resolution
    MinRRTScaled = MinRRT div Res,
    %% We calculate the cutoffdarte by substracting the minimum rr time
    %% from now, caluclating the in the most recent time past that we would
    %% still repair, anything larger then that we won't.
    Last = First + Count,

    case NowScaled - MinRRTScaled of
        %% When the last point read is smaller then the cutoff date
        %% all our points are within the RR area so we pass on the
        %% the default RR strategy
        %%
        %%         F==============>L
        %% t: 0 ---------------------C----------------->
        Cutoff when Last < Cutoff ->
            RROpt;
        %% If the cuttoff date is smaller then the first point read the entire
        %% section lies within the cutoff area and we don't do any RR
        %%
        %%                          F______________>L
        %% t: 0 -----------------C--------------------->
        Cuttoff when Cuttoff < First ->
            {rr, off};
        %% When the first point read is beyond the cutoff date, but the last
        %% point is within the cutoff range. We compute the partial repair
        %% section by substracting the first element of the cuttoff date
        %% calculating the number of points that are older then the cutoff
        %% and in result need repair.
        %%
        %%                F=======_______>L
        %% t: 0 -----------------C--------------------->
        Cutoff when First < Cutoff, Cutoff < Last,
                    PartialRepairs =:= true ->
            {rr, {partial, Cutoff - First}};
        %% By default, if we have at least one repair worthy point, we will
        %% use the default option.
        _ ->
            RROpt
    end;
apply_rtt(_MinRTT, _B, _T, _C, RROpt) ->
    RROpt.

do_send(Socket, Transport, B, M, T, C, Opts, S) ->
    S1 = otters:tag(S, <<"operation">>, <<"get">>),
    S2 = otters:tag(S1, <<"bucket">>, B),
    S3 = otters:tag(S2, <<"metric">>, M),
    S4 = otters:tag(S3, <<"offset">>, T),
    S5 = otters:tag(S4, <<"count">>, C),
    PPF = dalmatiner_opt:ppf(B),
    HPTS = proplists:get_bool(hpts, Opts),
    Splits = mstore:make_splits(T, C, PPF),
    S6 = otters:tag(S5, <<"splits">>, length(Splits)),
    %% We never send a aggregate for now.
    Transport:send(Socket, dproto_tcp:encode_get_reply({aggr, undefined})),
    S7 = otters:log(S6, "start sending parts"),
    send_parts(Socket, Transport, PPF, B, M, Opts, S7, HPTS, Splits).

send_parts(Socket, Transport, _PPF, _B, _M, _Opts, S, _HPTS, []) ->
    %% Reset the socket to 4 byte packages
    Transport:send(Socket, <<0>>),
    otters:log(S, "finish send");

send_parts(Socket, Transport, PPF, B, M, Opts, S, HPTS, [{T, C} | Splits]) ->
    S1 = otters:log(S, "get part"),
    {ok, Points} = metric:get(B, M, PPF, T, C, S1, Opts),
    send_part(Socket, Transport, C, Points, HPTS),
    S2 = otters:log(S1, "part send"),
    send_parts(Socket, Transport, PPF, B, M, Opts, S2, HPTS, Splits).

send_part(Socket, Transport, C, Points, HPTS) when is_integer(C),
                                                   is_binary(Points)->
    Padding = case HPTS of
                  false ->
                      C - mmath_bin:length(Points);
                  true ->
                      C - mmath_hpts:length(Points)
              end,
    Msg = {data, Points, Padding},
    Transport:send(Socket, dproto_tcp:encode_get_stream(Msg)).

-spec stream_loop(port(), term(), stream_state(),
                  {dproto_tcp:stream_message(), binary()}) ->
                         ok.
stream_loop(Socket, Transport,
            State = #sstate{
                       dict = Dict},
            {flush, Rest}) ->
    Dict1 = flush(Dict),
    stream_loop(Socket, Transport, State#sstate{dict = Dict1},
                dproto_tcp:decode_stream(Rest));

stream_loop(Socket, Transport,
            State = #sstate{dict = Dict, last = undefined,
                            max_bkt_batch_size = BDSize},
            {{stream, Metric, Time, Points}, Rest}) ->
    Dict1 = bkt_pdict:add(Metric, Time, Points, Dict),
    Dict2 = case bkt_pdict:size(Dict1) of
                X when X > BDSize ->
                    flush(Dict1);
                _ ->
                    Dict1
            end,
    stream_loop(Socket, Transport, State#sstate{dict = Dict2, last = Time},
                dproto_tcp:decode_stream(Rest));

stream_loop(Socket, Transport,
            State = #sstate{last = _L,
                            max_diff = _Max, dict = Dict},
            {{stream, Metric, Time, Points}, Rest})
  when Time - _L > _Max ->
    Dict1 = flush(Dict),
    Dict2 = bkt_pdict:add(Metric, Time, Points, Dict1),
    stream_loop(Socket, Transport, State#sstate{dict = Dict2, last = undefined},
                dproto_tcp:decode_stream(Rest));

stream_loop(Socket, Transport, State = #sstate{
                                          dict = Dict},
            {{stream, Metric, Time, Points}, Acc}) ->
    Dict1 = bkt_pdict:add(Metric, Time, Points, Dict),
    stream_loop(Socket, Transport, State#sstate{dict = Dict1},
                dproto_tcp:decode_stream(Acc));

stream_loop(Socket, Transport, State = #sstate{
                                          dict = Dict},
            {{batch, Time}, Acc}) ->
    %% When entering batch mode we make sure to drain the dict first and
    %% set last as undefined since we'll flush at the end too.
    %% TODO: figure out if this flushing makes sense or if we can make it
    %% conditional
    Dict1 = flush(Dict),
    batch_loop(Socket, Transport, State#sstate{dict = Dict1, last = undefined},
               Time, decode_batch(Acc, State));

stream_loop(Socket, Transport, State = #sstate{max_diff = D, recv_size = Size},
            {incomplete, Acc}) ->
    case Transport:recv(Socket, Size, min(D * 750, 5000)) of
        {ok, Data} ->
            Acc1 = <<Acc/binary, Data/binary>>,
            stream_loop(Socket, Transport, State#sstate{recv_size = Size + 64},
                        dproto_tcp:decode_stream(Acc1));
        {error, timeout} ->
            stream_loop(Socket, Transport,
                        State#sstate{recv_size = max(0, Size - 512)},
                        {incomplete, Acc});
        {error, closed} ->
            bkt_pdict:flush(State#sstate.dict),
            ok;
        E ->
            error(E, Transport, Socket, State)
    end.

-spec batch_loop(port(), term(), stream_state(), non_neg_integer(),
                 {dproto_tcp:batch_message(), binary()}) ->
                        ok.

decode_batch(Data, #sstate{hpts = true}) ->
    dproto_tcp:decode_batch_hpts(Data);
decode_batch(Data, #sstate{hpts = false}) ->
    dproto_tcp:decode_batch(Data).

batch_loop(Socket, Transport, State = #sstate{dict = Dict}, _Time,
           {batch_end, Acc}) ->
    %io:format("Batch end: ~p\n", [Dict]),
    Dict1 = flush(Dict),
    stream_loop(Socket, Transport, State#sstate{dict = Dict1},
                dproto_tcp:decode_stream(Acc));

batch_loop(Socket, Transport, State  = #sstate{dict = Dict}, Time,
           {{batch, Metric, Point}, Acc}) ->
    Dict1 = bkt_pdict:add(Metric, Time, Point, Dict),
    batch_loop(Socket, Transport, State#sstate{dict = Dict1}, Time,
               decode_batch(Acc, State));

batch_loop(Socket, Transport, State  = #sstate{dict = Dict}, Time,
           {{batch_hpts, Metric, Point}, Acc}) ->
    Dict1 = bkt_pdict:add(Metric, Time, Point, Dict),
    batch_loop(Socket, Transport, State#sstate{dict = Dict1}, Time,
               decode_batch(Acc, State));


batch_loop(Socket, Transport, State, Time, {incomplete, Acc}) ->
    case Transport:recv(Socket, 0, 1000) of
        {ok, Data} ->
            Acc1 = <<Acc/binary, Data/binary>>,
            batch_loop(Socket, Transport, State, Time,
                       decode_batch(Acc1, State));
        {error, timeout} ->
            batch_loop(Socket, Transport, State, Time, {incomplete, Acc});
        {error, closed} ->
            bkt_pdict:flush(State#sstate.dict),
            ok;
        E ->
            error(E, Transport, Socket, State)
    end.

flush(Dict) ->
    Dict1 = bkt_pdict:flush(Dict),
    drain(),
    Dict1.

drain() ->
    receive
        _ ->
            drain()
    after
        0 ->
            ok
    end.

error(E, Transport, Socket, #sstate{
                               dict = Dict}) ->
    lager:error("[tcp:stream] Error: ~p~n", [E]),
    bkt_pdict:flush(Dict),
    ok = Transport:close(Socket).
get_events(_Bucket, _Filter, [], Socket, Transport, State) ->
    Transport:send(Socket, dproto_tcp:encode(events_end)),
    loop(Socket, Transport, State);

get_events(Bucket, Filter, [{Start, End} | R], Socket, Transport, State) ->
    {ok, Es} = event:get(Bucket, Start, End, Filter),
    Es1 = [{T, E} || {T, _, E} <- Es],
    Transport:send(Socket, dproto_tcp:encode({events, Es1})),
    get_events(Bucket, Filter, R, Socket, Transport, State).

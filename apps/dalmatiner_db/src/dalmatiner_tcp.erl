-module(dalmatiner_tcp).

-behaviour(ranch_protocol).

-include_lib("dproto/include/dproto.hrl").
-include_lib("mmath/include/mmath.hrl").

-export([start_link/4]).
-export([init/4]).

-record(state,
        {n = 1 :: pos_integer(),
         w = 1 :: pos_integer(),
         rr_min_time = 0 :: non_neg_integer()
        }).

-record(sstate,
        {last = undefined :: non_neg_integer() | undefined,
         max_diff = 1 :: pos_integer(),
         dict :: bkt_dict:bkt_dict()}).


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
            case dproto_tcp:decode(Data) of
                buckets ->
                    Bs = dalmatiner_bucket:list(),
                    Transport:send(Socket, dproto_tcp:encode_metrics(Bs)),
                    loop(Socket, Transport, State);
                {list, Bucket} ->
                    {ok, Ms} = metric:list(Bucket),
                    Transport:send(Socket, dproto_tcp:encode_metrics(Ms)),
                    loop(Socket, Transport, State);
                {delete, Bucket} ->
                    metric:delete(Bucket),
                    loop(Socket, Transport, State);
                {list, Bucket, Prefix} ->
                    {ok, Ms} = metric:list(Bucket, Prefix),
                    Transport:send(Socket, dproto_tcp:encode_metrics(Ms)),
                    loop(Socket, Transport, State);
                {info, Bucket} ->
                    Info = dalmatiner_bucket:info(Bucket),
                    InfoBin = dproto_tcp:encode_bucket_info(Info),
                    Transport:send(Socket, InfoBin),
                    loop(Socket, Transport, State);
                {get, B, M, T, C, Opts} ->
                    Opts1 =  apply_rtt(State#state.rr_min_time, B, T, Opts),
                    do_send(Socket, Transport, B, M, T, C, Opts1),
                    loop(Socket, Transport, State);
                {ttl, Bucket, TTL} ->
                    ok = metric:update_ttl(Bucket, TTL),
                    loop(Socket, Transport, State);
                {events, Bucket, Es} ->
                    event:append(Bucket, Es),
                    loop(Socket, Transport, State);
                {get_events, Bucket, Start, End} ->
                    Size = event:split(Bucket),
                    Splits = estore:make_splits(Start, End, Size),
                    get_events(Bucket, [], Splits, Socket, Transport, State);
                {get_events, Bucket, Start, End, Filter} ->
                    Size = event:split(Bucket),
                    Splits = estore:make_splits(Start, End, Size),
                    get_events(Bucket, Filter, Splits, Socket, Transport,
                               State);
                {stream, Bucket, Delay} ->
                    lager:info("[tcp] Entering stream mode for bucket '~s' "
                               "and a max delay of: ~p", [Bucket, Delay]),
                    ok = Transport:setopts(Socket, [{packet, 0}]),
                    stream_loop(Socket, Transport,
                                #sstate{max_diff = Delay,
                                        dict = bkt_dict:new(Bucket,
                                                            State#state.n,
                                                            State#state.w)},
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
%% option will be honoured.  Otherwise, if the cutoff date (now - min RTT) is
%% larger then the first timestamp of the read, we have data that is older
%% and will perform a read repair.
apply_rtt(MinRTT, B, T, Opts) when is_list(Opts) ->
    RROpt = proplists:lookup(rr, Opts),
    Opts1 = proplists:delete(rr, Opts),
    [apply_rtt(MinRTT, B, T, RROpt) | Opts1];
apply_rtt(0, _B, _T, RROpt) ->
    RROpt;
apply_rtt(MinRTT, B, T, {rr, default} = RROpt) ->
    Now = erlang:system_time(milli_seconds) div dalmatiner_opt:resolution(B),
    case Now - MinRTT of
        Cutoff when Cutoff > T ->
            RROpt;
        _ ->
            {rr, off}
    end;
apply_rtt(_MinRTT, _B, _T, RROpt) ->
    RROpt.

do_send(Socket, Transport, B, M, T, C, Opts) ->
    PPF = dalmatiner_opt:ppf(B),
    Splits = mstore:make_splits(T, C, PPF),
    send_parts(Socket, Transport, PPF, B, M, Opts, Splits).

send_parts(Socket, Transport, _PPF, _B, _M, _Opts, []) ->
    %% Reset the socket to 4 byte packages
    Transport:send(Socket, <<0>>);

send_parts(Socket, Transport, PPF, B, M, Opts, [{T, C} | Splits]) ->
    {ok, Points} = metric:get(B, M, PPF, T, C, Opts),
    send_part(Socket, Transport, C, Points),
    send_parts(Socket, Transport, PPF, B, M, Opts, Splits).

send_part(Socket, Transport, C, Points) when is_integer(C),
                                              is_binary(Points)->
    {ok, Compressed} = snappyer:compress(Points),
    case C - mmath_bin:length(Points) of
        0 ->
            Transport:send(Socket, <<1, Compressed/binary>>);
        Padding ->
            Transport:send(Socket, <<2, Padding:64/integer, Compressed/binary>>)
    end.

-spec stream_loop(port(), term(), stream_state(),
                  {dproto_tcp:stream_message(), binary()}) ->
                         ok.
stream_loop(Socket, Transport,
            State = #sstate{dict = Dict},
            {flush, Rest}) ->
    Dict1 = flush(Dict),
    stream_loop(Socket, Transport, State#sstate{dict = Dict1},
                dproto_tcp:decode_stream(Rest));

stream_loop(Socket, Transport,
            State = #sstate{dict = Dict, last = undefined},
            {{stream, Metric, Time, Points}, Rest}) ->
    Dict1 = bkt_dict:add(Metric, Time, Points, Dict),
    stream_loop(Socket, Transport, State#sstate{dict = Dict1, last = Time},
                dproto_tcp:decode_stream(Rest));

stream_loop(Socket, Transport,
            State = #sstate{last = _L, max_diff = _Max, dict = Dict},
            {{stream, Metric, Time, Points}, Rest})
  when Time - _L > _Max ->
    Dict1 = flush(Dict),
    Dict2 = bkt_dict:add(Metric, Time, Points, Dict1),
    stream_loop(Socket, Transport, State#sstate{dict = Dict2, last = undefined},
                dproto_tcp:decode_stream(Rest));

stream_loop(Socket, Transport, State = #sstate{dict = Dict},
            {{stream, Metric, Time, Points}, Acc}) ->
    Dict1 = bkt_dict:add(Metric, Time, Points, Dict),
    stream_loop(Socket, Transport, State#sstate{dict = Dict1},
                dproto_tcp:decode_stream(Acc));

stream_loop(Socket, Transport, State = #sstate{dict = Dict},
            {{batch, Time}, Acc}) ->
    %% When entering batch mode we make sure to drain the dict first and
    %% set last as undefined since we'll flush at the end too.
    %% TODO: figure out if this flushing makes sense or if we can make it
    %% conditional
    Dict1 = flush(Dict),
    batch_loop(Socket, Transport, State#sstate{dict = Dict1, last = undefined},
               Time, dproto_tcp:decode_batch(Acc));

stream_loop(Socket, Transport, State = #sstate{max_diff = D},
            {incomplete, Acc}) ->
    case Transport:recv(Socket, 0, min(D * 1000, 5000)) of
        {ok, Data} ->
            Acc1 = <<Acc/binary, Data/binary>>,
            stream_loop(Socket, Transport, State,
                        dproto_tcp:decode_stream(Acc1));
        {error, timeout} ->
            stream_loop(Socket, Transport, State, {incomplete, Acc});
        {error, closed} ->
            bkt_dict:flush(State#sstate.dict),
            ok;
        E ->
            error(E, Transport, Socket, State)
    end.

-spec batch_loop(port(), term(), stream_state(), non_neg_integer(),
                 {dproto_tcp:batch_message(), binary()}) ->
                        ok.

batch_loop(Socket, Transport, State = #sstate{dict = Dict}, _Time,
           {batch_end, Acc}) ->
    Dict1 = flush(Dict),
    stream_loop(Socket, Transport, State#sstate{dict = Dict1},
               dproto_tcp:decode_stream(Acc));

batch_loop(Socket, Transport, State  = #sstate{dict = Dict}, Time,
           {{batch, Metric, Point}, Acc}) ->
    Dict1 = bkt_dict:add(Metric, Time, Point, Dict),
    batch_loop(Socket, Transport, State#sstate{dict = Dict1}, Time,
               dproto_tcp:decode_batch(Acc));


batch_loop(Socket, Transport, State, Time, {incomplete, Acc}) ->
    case Transport:recv(Socket, 0, 1000) of
        {ok, Data} ->
            Acc1 = <<Acc/binary, Data/binary>>,
            batch_loop(Socket, Transport, State, Time,
                        dproto_tcp:decode_batch(Acc1));
        {error, timeout} ->
            batch_loop(Socket, Transport, State, Time, {incomplete, Acc});
        {error, closed} ->
            bkt_dict:flush(State#sstate.dict),
            ok;
        E ->
            error(E, Transport, Socket, State)
    end.

flush(Dict) ->
    Dict1 = bkt_dict:flush(Dict),
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

error(E, Transport, Socket, #sstate{dict = Dict}) ->
    lager:error("[tcp:stream] Error: ~p~n", [E]),
    bkt_dict:flush(Dict),
    ok = Transport:close(Socket).
get_events(_Bucket, _Filter, [], Socket, Transport, State) ->
    Transport:send(Socket, dproto_tcp:encode(events_end)),
    loop(Socket, Transport, State);

get_events(Bucket, Filter, [{Start, End} | R], Socket, Transport, State) ->
    {ok, Es} = event:get(Bucket, Start, End, Filter),
    Es1 = [{T, E} || {T, _, E} <- Es],
    Transport:send(Socket, dproto_tcp:encode({events, Es1})),
    get_events(Bucket, Filter, R, Socket, Transport, State).

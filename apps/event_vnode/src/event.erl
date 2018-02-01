-module(event).

-export([
         delete/1,
         append/2,
         get/4,
         split/1
        ]).

-ignore_xref([get/4]).

delete(Bucket) ->
    ddb_histogram:timed_update(
      list_metrics, event_coverage, start, [{delete, Bucket}]).

append(_Bucket, []) ->
    ok;
append(Bucket, [{T, E} | Events]) ->
    {ok, N} = application:get_env(dalmatiner_db, n),
    {ok, W} = application:get_env(dalmatiner_db, w),
    Split = split(Bucket),
    append(N, W, Split, Bucket, Events, T div Split, [{T, estore:eid(), E}]).

append(N, W, _Split, Bucket, [], C, Acc) ->
    do_append(N, W, Bucket, C, Acc);

append(N, W, Split, Bucket, [{T, E} | Es], C, Acc)
  when T div Split =:= C ->
    append(N, W, Split, Bucket, Es, C, [{T, estore:eid(), E} | Acc]);
append(N, W, Split, Bucket, [{T, E} | Es], C, Acc) ->
    %% If we have to write over multiple VNodes
    %% We do it asyncronously for all but the 'last'
    %% one. This way we do get some back bpressure but
    %% most writes are syncronous (lets see how that turns out)...
    spawn(fun() ->
                  do_append(N, W, Bucket, C, Acc)
         end),
    append(N, W, Split, Bucket, Es, T div Split, [{T, estore:eid(), E}]).


do_append(N, W, Bucket, C, Events) ->
    DocIdx = riak_core_util:chash_key({Bucket, C}),
    Preflist = riak_core_apl:get_apl(DocIdx, N, event),
    ReqID = make_ref(),
    event_vnode:put(Preflist, ReqID, Bucket, Events),
    ddb_histogram:timed_update(
      {event, put},
      fun() ->
              do_wait(W, ReqID)
      end).

get(Bucket, Start, End, Filter) ->
    get(Bucket, split(Bucket), Start, End, Filter).

get(Bucket, Split, Start, End, Filter) when
      Start div Split =:= End div Split->
    ddb_histogram:timed_update(
      {event, get}, dalmatiner_read_fsm, start,
      [{event_vnode, event}, get, {Bucket, Start div Split},
       {Start, End, Filter}]).

do_wait(0, _ReqID) ->
    ok;

do_wait(W, ReqID) ->
    receive
        {ReqID, ok} ->
            do_wait(W - 1, ReqID)
    after
        5000 ->
            {error, timeout}
    end.

split(Bucket) ->
    PPF = dalmatiner_opt:ppf(Bucket),
    Res = dalmatiner_opt:resolution(Bucket),
    erlang:convert_time_unit(PPF * Res, milli_seconds, nano_seconds).

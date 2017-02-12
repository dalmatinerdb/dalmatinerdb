-module(ddb_counter).

-export([init/0, register/1, get_and_clean/0, inc/1, inc/2]).

-define(COUNTERS, ddb_counters).

%%%===================================================================
%%% API
%%%===================================================================

init() ->
    ets:new(?COUNTERS,
            [named_table, set, public, {write_concurrency, true}]).

register(_Name) ->
    ok.

inc(Type) ->
    inc(Type, 1).

inc(Type, N) when is_atom(Type) ->
    inc(atom_to_binary(Type, utf8), N);

inc(Type, N) when is_binary(Type) ->
    try
        ets:update_counter(?COUNTERS, {Type, self()}, N)
    catch
        error:badarg ->
            ets:insert(?COUNTERS, {{Type, self()}, N})
    end,
    ok.

get_and_clean() ->
    case ets:tab2list(?COUNTERS) of
        [] ->
            [];
        Results ->
            ets:delete_all_objects(?COUNTERS),
            Results1 = [{Name, Cnt} || {{_, Name}, Cnt} <- Results],
            collaps_counters(lists:sort(Results1))
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-record(fold_acc,
        {
          name,
          count,
          results = []
        }).

fold_counters({Name, Count}, Acc = #fold_acc{name = Name, count = CountAcc}) ->
    Acc#fold_acc{count = CountAcc + Count};
fold_counters({Name, Count},
              Acc = #fold_acc{name = AName, count = ACount, results = R}) ->
    Acc#fold_acc{results = [{AName, ACount} | R], name = Name, count = Count}.

collaps_counters([{Name, Count} | Counters]) ->
    Acc = #fold_acc{name = Name, count = Count},
    #fold_acc{results = R} = lists:foldl(fun fold_counters/2, Acc, Counters),
    R;
collaps_counters([]) ->
    [].

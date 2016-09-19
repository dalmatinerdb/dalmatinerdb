%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2016, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 19 Sep 2016 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(event_filter).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type matcher() ::
        '==' | '>=' | '=<' | '>' | '<' | '~='.
-type path() ::
        binary() | [binary() | integer()].
-type filter() ::
        {matcher(), path(), term()}
        | {defined, path()}.

-type filters() ::
        [filter() | {'or', filters(), filters()}].

-export([filter/2]).

-spec filter(map(), filters()) ->
                    boolean().
filter(_, []) ->
    true;
filter(O, [{'or', F1, F2} | R]) ->
    (filter(O, F1) orelse filter(O, F2))
        andalso filter(O, R);
filter(O, [{'not', F} | R]) ->
    (not filter_(O, F))
        andalso filter(O, R);
filter(O, [F | R]) ->
    filter_(O, F)
        andalso filter(O, R).


filter_(O, {defined, P}) ->
    jsxd:get(P, O) =/= undefined;
filter_(O, {'==', P, V}) ->
    case jsxd:get(P, O) of
        {ok, V1} ->
            V1 == V;
        _ ->
            false
    end;
filter_(O, {'>=', P, V}) ->
    case jsxd:get(P, O) of
        {ok, V1} ->
            V1 >= V;
        _ ->
            false
    end;
filter_(O, {'>', P, V}) ->
    case jsxd:get(P, O) of
        {ok, V1} ->
            V1 > V;
        _ ->
            false
    end;
filter_(O, {'=<', P, V}) ->
    case jsxd:get(P, O) of
        {ok, V1} ->
            V1 =< V;
        _ ->
            false
    end;
filter_(O, {'<', P, V}) ->
    case jsxd:get(P, O) of
        {ok, V1} ->
            V1 =< V;
        _ ->
            false
    end;
filter_(O, {'~=', P, Re}) ->
    case jsxd:get(P, O) of
        {ok, V1} ->
            re:run(V1, Re, [{capture, none}]) =:= match;
        _ ->
            false
    end.

-ifdef(TEST).

eqal_test() ->
    ?assert(filter(#{a => 1}, [{'==', [a], 1}])),
    ?assert(not filter(#{a => 1}, [{'==', [a], 2}])).
-endif.

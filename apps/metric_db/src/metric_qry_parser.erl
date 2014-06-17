-module(metric_qry_parser).

-export([parse/1, unparse/1, execute/1, glob_match/2, rmatch/2]).
-ignore_xref([parse/1, unparse/1, execute/1, glob_match/2, rmatch/2]).

date(L) ->
    i(L).

i(L) ->
    list_to_integer(L).

f(L) ->
    list_to_float(L).

b(L) ->
    list_to_binary(L).

parse(B) when is_binary(B) ->
    parse(binary_to_list(B));

parse(L) when is_list(L) ->
    Tokens = string:tokens(L, " "),
    initial(Tokens).

initial(["SELECT" | L]) ->
    range(L);
initial(L) ->
    {error, undefeind, L}.

range(["BETWEEN", A, "AND", B | L]) ->
    Ad = date(A),
    metric(L, {range, Ad, date(B) - Ad});
range(L) ->
    {error, undefeind, L}.

metric(["FROM", "SUM", "OF", M | L], Acc) ->
    aggregate(L, {mget, sum, b(M), Acc});

metric(["FROM", "AVG", "OF", M | L], Acc) ->
    aggregate(L, {mget, avg, b(M), Acc});

metric(["FROM", M | L], Acc) ->
    aggregate(L, {get, b(M), Acc});
metric(L, Acc) ->
    {error, Acc, L}.

aggregate([], Acc) ->
    Acc;
aggregate(["AS", "LIST"], Acc) ->
    {to_list, Acc};
aggregate(["DERIVATE" | L], Acc) ->
    aggregate(L, {derivate, Acc});
aggregate(["SCALE", "BY", S | L], Acc) ->
    aggregate(L, {scale, f(S), Acc});

aggregate(["MIN", "OF", N | L], Acc) ->
    aggregate(L, {min, i(N), Acc});
aggregate(["MAX", "OF", N | L], Acc) ->
    aggregate(L, {max, i(N), Acc});

aggregate(["AVG", "OVER", N | L], Acc) ->
    aggregate(L, {avg, i(N), Acc});
aggregate(["SUM", "OVER", N | L], Acc) ->
    aggregate(L, {sum, i(N), Acc});
aggregate(L, Acc) ->
    {error, Acc, L}.


unparse({to_list, T}) ->
    unparse(T, "AS LIST");

unparse(T) ->
    unparse(T, []).

unparse({range, A, B}, Acc) ->
    lists:flatten(["SELECT BETWEEN ", integer_to_list(A), " AND ",
                   integer_to_list(A+B), " " | Acc]);

unparse({mget, sum, M, C}, Acc) ->
    unparse(C, ["FROM SUM OF ", binary_to_list(M), " " | Acc]);

unparse({mget, avg, M, C}, Acc) ->
    unparse(C, ["FROM AVG OF ", binary_to_list(M), " " | Acc]);

unparse({get, M, C}, Acc) ->
    unparse(C, ["FROM ", binary_to_list(M), " " | Acc]);

unparse({derivate, C}, Acc) ->
    unparse(C, ["DERIVATE " | Acc]);

unparse({scale, S, C}, Acc) ->
    unparse(C, ["SCALE BY ", float_to_list(S), " " | Acc]);

unparse({min, N, C}, Acc) ->
    unparse(C, ["MIN OF ", integer_to_list(N), " " | Acc]);
unparse({max, N, C}, Acc) ->
    unparse(C, ["MAX OF ", integer_to_list(N), " " | Acc]);

unparse({avg, N, C}, Acc) ->
    unparse(C, ["AVG OVER ", integer_to_list(N), " " | Acc]);
unparse({sum, N, C}, Acc) ->
    unparse(C, ["SUM OVER ", integer_to_list(N), " " | Acc]).

execute({mget, sum, G, {range, A, B}}) ->
    {ok, Ms} = metric:list(),
    Ms1 = glob_match(G, Ms),
    mmath_comb:sum([begin {ok, V} = metric:get(M, A, B), V end || M <- Ms1]);

execute({mget, avg, G, {range, A, B}}) ->
    {ok, Ms} = metric:list(),
    Ms1 = glob_match(G, Ms),
    mmath_comb:avg([begin {ok, V} = metric:get(M, A, B), V end || M <- Ms1]);

execute({get, M, {range, A, B}}) ->
    {ok, V} = metric:get(M, A, B),
    V;
execute({derivate, C}) ->
    mmath_aggr:derivate(execute(C));
execute({scale, S, C}) ->
    mmath_aggr:scale(execute(C), S);
execute({min, N, C}) ->
    mmath_aggr:min(execute(C), N);
execute({max, N, C}) ->
    mmath_aggr:max(execute(C), N);
execute({avg, N, C}) ->
    mmath_aggr:avg(execute(C), N);
execute({sum, N, C}) ->
    mmath_aggr:sum(execute(C), N);

execute({to_list, C}) ->
    D = execute(C),
    L = mmath_bin:to_list(D),
    case mmath_bin:find_type(D) of
        float ->
            << <<(f2b(V))/binary, " ">> || V <- L >>;
        _ ->
            << <<(i2b(V))/binary, " ">> || V <- L >>
    end.


i2b(I) ->
    list_to_binary(integer_to_list(I)).

f2b(I) ->
    list_to_binary(float_to_list(I)).

glob_match(G, Ms) ->
    GE = re:split(G, "\\*"),
    F = fun(M) ->
                rmatch(GE, M)
        end,
    lists:filter(F, Ms).


rmatch([<<>>, <<$., Ar1/binary>> | Ar], B) ->
    rmatch([Ar1 | Ar], skip_one(B));
rmatch([<<>> | Ar], B) ->
    rmatch(Ar, skip_one(B));
rmatch([<<$., Ar1/binary>> | Ar], B) ->
    rmatch([Ar1 | Ar], skip_one(B));
rmatch([A | Ar], B) ->
    S = byte_size(A),
    case B of
        <<A:S/binary, Br/binary>> ->
            rmatch(Ar, Br);
        _ ->
            false
    end;
rmatch([], <<>>) ->
    true;
rmatch(_A, _B) ->
    false.

skip_one(<<$., R/binary>>) ->
    R;
skip_one(<<>>) ->
    <<>>;
skip_one(<<_, R/binary>>) ->
    skip_one(R).


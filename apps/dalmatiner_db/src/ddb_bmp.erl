-module(ddb_bmp).

%% for CLI
-export([show/1, compare/1, repair/1]).
-ignore_xref([show/1, compare/1, repair/1]).


show(["--width", WidthS, TimeS, BucketS | MetricS]) ->
    Width = list_to_integer(WidthS),
    show_bitmap(TimeS, BucketS,  MetricS, Width);
show([TimeS, BucketS | MetricS]) ->
    show_bitmap(TimeS, BucketS,  MetricS, 100).

compare(["--width", WidthS, TimeS, BucketS | MetricS]) ->
    Width = list_to_integer(WidthS),
    compare_nodes(TimeS, BucketS,  MetricS, Width);
compare([TimeS, BucketS | MetricS]) ->
    compare_nodes(TimeS, BucketS,  MetricS, 100).

repair([TimeS, BucketS | MetricS]) ->
    Bucket = list_to_binary(BucketS),
    Time = list_to_integer(TimeS),
    MetricL = [list_to_binary(M) || M <- MetricS],
    Metric = dproto:metric_from_list(MetricL),
    PPF = dalmatiner_opt:ppf(Bucket),
    Base = Time div PPF,
    {ok, M} = metric:get(Bucket, Metric, PPF, Base*PPF, PPF),
    io:format("Read ~p datapoints.~n", [mmath_bin:length(M)]).


%%====================================================================
%% Internal functions
%%====================================================================


get_bmps(Bucket, Metric, Time) ->
    Nodes =  get_nodes(Bucket, Metric, Time),
    get_nodes(Nodes, Bucket, Metric, Time, []).

compare_nodes(TimeS, BucketS, MetricS, Width) ->
    Bucket = list_to_binary(BucketS),
    Time = list_to_integer(TimeS),
    MetricL = [list_to_binary(M) || M <- MetricS],
    Metric = dproto:metric_from_list(MetricL),
    Results = [{_, _, B0} | Rest] = get_bmps(Bucket, Metric, Time),
    Union = calc_f(fun bitmap:union/2, B0, Rest),
    Intersection = calc_f(fun bitmap:intersection/2, B0, Rest),
    case {Union, Intersection} of
        {_R, _R} ->
            io:format("No difference~n");
        _ ->
            io:format("Total difference:~n"),
            bitmap:display_diff(Union, Intersection, Width),
            show_diff(Results, Union, Width),
            io:format("~n")
    end.

show_diff([], _Union, _Width) ->
    ok;
show_diff([{Node, P, not_found} | R], Union, Width) ->
    io:format("~n=== ~s (~p)~n  This node had no data~n", [Node, P]),
    show_diff(R, Union, Width);
show_diff([{Node, P, Union} | R], Union, Width) ->
    io:format("~n=== ~s (~p)~n"
              "* No missing points~n", [Node, P]),
    show_diff(R, Union, Width);
show_diff([{Node, P, Bitmap} | R], Union, Width) ->
    io:format("~n=== ~s (~p)~n", [Node, P]),
    bitmap:display_diff(Union, Bitmap, Width),
    show_diff(R, Union, Width).

calc_f(_F, R, []) ->
    R;
calc_f(F, not_found, [{_, _, not_found} | R])->
    calc_f(F, not_found, R);
calc_f(F, not_found, [{_, _, B0} | R])->
    {ok, B1} = bitmap:new([{size, bitmap:size(B0)}]),
    calc_f(F, F(B0, B1), R);
calc_f(F, B0, [{_, _, not_found} | R]) ->
    {ok, B1} = bitmap:new([{size, bitmap:size(B0)}]),
    calc_f(F, F(B0, B1), R);
calc_f(F, B0, [{_, _, B1} | R]) ->
    calc_f(F, F(B0, B1), R).

get_nodes([], _Bucket, _Metric, _Time, Acc) ->
    Acc;
get_nodes([{P, N} = PN | R], Bucket, Metric, Time, Acc) ->
    case metric_vnode:get_bitmap(PN, Bucket, Metric, Time) of
        {ok, BMP} ->
            get_nodes(R, Bucket, Metric, Time, [{N, P, BMP} | Acc]);
        _O ->
            get_nodes(R, Bucket, Metric, Time, [{N, P, not_found} | Acc])
    end.

show_bitmap(TimeS, BucketS, MetricS, Width) ->
    Bucket = list_to_binary(BucketS),
    Time = list_to_integer(TimeS),
    MetricL = [list_to_binary(M) || M <- MetricS],
    Metric = dproto:metric_from_list(MetricL),
    Nodes =  get_bmps(Bucket, Metric, Time),
    case [{P, BMP} || {Node, P, BMP} <- Nodes, Node =:= node()] of
        [] ->
            io:format("No valid node found, try on: ~p~n",
                      [[Node || {Node, _, _} <- Nodes]]),
            error;
        L ->
            lists:map(fun({P, not_found}) ->
                               io:format("=== ~p~n", [P]),
                               io:format("* No data for this time range~n");
                          ({P, BMP}) ->
                               io:format("=== ~p~n", [P]),
                               bitmap:display(BMP, Width),
                               io:format("~n")
                       end, L)
    end.

get_nodes(Bucket, Metric, Time) ->
    PPF = dalmatiner_opt:ppf(Bucket),
    {ok, N} = application:get_env(dalmatiner_db, n),
    Base = Time div PPF,
    DocIdx = riak_core_util:chash_key({Bucket, {Metric, Base}}),
    riak_core_apl:get_apl(DocIdx, N, metric).

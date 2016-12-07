-module(ddb_bmp).

%% for CLI
-export([show/1, verify/1]).

%% for RPC
-export([compile/3, get/4]).

-ignore_xref([show/1, verify/1, compile/3, get/4]).


show(["--width", WidthS, TimeS, BucketS | MetricS]) ->
    Width = list_to_integer(WidthS),
    show_bitmap(TimeS, BucketS,  MetricS, Width);
show([TimeS, BucketS | MetricS]) ->
    show_bitmap(TimeS, BucketS,  MetricS, 100).

verify(["--width", WidthS, TimeS, BucketS | MetricS]) ->
    Width = list_to_integer(WidthS),
    compare_nodes(TimeS, BucketS,  MetricS, Width);
verify([TimeS, BucketS | MetricS]) ->
    compare_nodes(TimeS, BucketS,  MetricS, 100).


%%====================================================================
%% Internal functions
%%====================================================================

compare_nodes(TimeS, BucketS, MetricS, Width) ->
    Bucket = list_to_binary(BucketS),
    Time = list_to_integer(TimeS),
    MetricL = [list_to_binary(M) || M <- MetricS],
    Metric = dproto:metric_from_list(MetricL),
    Nodes =  get_nodes(Bucket, Metric, Time),
    Results = [{_, B0} | Rest] = get_nodes(Nodes, Bucket, Metric, Time, []),
    Union = calc_f(fun bitmap:union/2, B0, Rest),
    Intersection = calc_f(fun bitmap:intersection/2, B0, Rest),
    case {Union, Intersection} of
        {_R, _R} ->
            io:format("No difference~n");
        _ ->
            io:format("Total difference:~n"),
            bitmap:display_diff(Union, Intersection, Width),
            io:format("~n", []),
            show_diff(Results, Union, Width)
    end.

show_diff([], _Union, _Width) ->
    ok;
show_diff([{Node, not_found} | R], Union, Width) ->
    io:format("~n=== ~s~n  This node had no data~n", [Node]),
    show_diff(R, Union, Width);
show_diff([{Node, Union} | R], Union, Width) ->
    io:format("~n=== ~s~n"
              "* No missing points~n", [Node]),
    show_diff(R, Union, Width);
show_diff([{Node, Bitmap} | R], Union, Width) ->
    io:format("~n=== ~s~n", [Node]),
    bitmap:display_diff(Union, Bitmap, Width),
    show_diff(R, Union, Width).

calc_f(_F, R, []) ->
    R;
calc_f(F, not_found, [{_, B0} | R])->
    calc_f(F, B0, R);
calc_f(F, B0, [{_, not_found} | R]) ->
    calc_f(F, B0, R);
calc_f(F, B0, [{_, B1} | R]) ->
    calc_f(F, F(B0, B1), R).

get_nodes([], _Bucket, _Metric, _Time, Acc) ->
    Acc;
get_nodes([{P, N} | R], Bucket, Metric, Time, Acc) ->
    rpc:call(N, ddb_bmp, compile, [P, Bucket, Time]),
    case rpc:call(N, ddb_bmp, get, [P, Bucket, Metric, Time]) of
        {ok, BMP} ->
            get_nodes(R, Bucket, Metric, Time, [{N, BMP} | Acc]);
        {error, not_found} ->
            get_nodes(R, Bucket, Metric, Time, [{N, not_found} | Acc])
    end.

show_bitmap(TimeS, BucketS, MetricS, Width) ->
    Bucket = list_to_binary(BucketS),
    Time = list_to_integer(TimeS),
    MetricL = [list_to_binary(M) || M <- MetricS],
    Metric = dproto:metric_from_list(MetricL),
    Nodes =  get_nodes(Bucket, Metric, Time),
    case [P || {P, Node} <- Nodes, Node =:= node()] of
        [] ->
            io:format("No valid node found, try on: ~p~n",
                      [[Node || {_, Node} <- Nodes]]),
            error;
        [P] ->
            compile(P, Bucket, Time),
            case get(P, Bucket, Metric, Time) of
                {ok, BMP} ->
                    io:format("=== ~s~n", [string:join(MetricS, ".")]),
                    bitmap:display(BMP, Width),
                    io:format("~n");
                {error, not_found} ->
                    io:format("No data for this time range~n"),
                    error
            end
    end.

file(Hash, Bucket, Time) ->
    PPF = dalmatiner_opt:ppf(Bucket),
    Base = Time div PPF,
    {ok, DD} = application:get_env(riak_core, platform_data_dir),
    binary_to_list(filename:join([DD, integer_to_list(Hash), Bucket,
                                  integer_to_list(Base*PPF)])).

get(Hash, Bucket, Metric, Time) ->
    F = file(Hash, Bucket, Time),
    mstore_inspector:get(F, Metric).

compile(Hash, Bucket, Time) ->
    F = file(Hash, Bucket, Time),
    mstore_inspector:create(F),
    F.

get_nodes(Bucket, Metric, Time) ->
    PPF = dalmatiner_opt:ppf(Bucket),
    {ok, N} = application:get_env(dalmatiner_db, n),
    Base = Time div PPF,
    DocIdx = riak_core_util:chash_key({Bucket, {Metric, Base}}),
    riak_core_apl:get_apl(DocIdx, N, metric).

-module(metric_cache_cli).
-behavior(clique_handler).

-export([register_cli/0]).

-spec register_cli() -> ok.
register_cli() ->
    %register_cli_usage(),
    %% register_cli_cfg(),
    register_cli_cmds(),
    %%register_config_whitelist(),
    ok.

register_cli_cmds() ->
    ok = clique:register_command(["ddb-admin", "cache", "stats"], [],
                                 node_and_all_flags(), fun cache_stats/3).

node_and_all_flags() ->
    [{node, [{shortname, "n"}, {longname, "node"},
             {typecast, fun clique_typecast:to_node/1}]},
     {all, [{shortname, "a"}, {longname, "all"}]}].


cache_stats(_CmdBase, _Args, Flags) when length(Flags) > 1 ->
    [clique_status:text("Can't specify both --all and --node flags")];
cache_stats(_CmdBase, _Args, []) ->
    Stats = metric:cache_stats(),
    [clique_status:text([print_cache_stats(Stat) || Stat <- Stats])];
cache_stats(CmdBase, Args, [{all, _Val}]) ->
    cache_stats(CmdBase, Args, []);
    %% We can't execute on any node
    %%clique_config:show(config_vars(), [{all, Val}]);
cache_stats(CmdBase, Args, [{node, _Node}]) ->
    cache_stats(CmdBase, Args, []).
    %% We can't execute on all nodes al
    %%clique_config:show(config_vars(), [{node, Node}]).

print_bucket({Name, B}) ->
    Age = proplists:get_value(age, B),
    Inserts = proplists:get_value(total_inserts, B),
    Evictions = proplists:get_value(evictions, B),
    Count = proplists:get_value(count, B),
    Alloc = proplists:get_value(alloc, B),
    [io_lib:format("  ~s:~n", [Name]),
     io_lib:format("  ~5B | ~15B | ~15B | ~15B | ~10B ~n",
               [Age, Inserts, Evictions, Count, Alloc])].

print_cache_stats({I, C}) ->
    Count = proplists:get_value(count, C),
    Alloc = proplists:get_value(alloc, C),
    case proplists:get_value(buckets, C) of
        [] ->
            [];
    Buckets ->
            [io_lib:format("~p: [~p byte / ~p elements]~n", [I, Alloc, Count]),
             "  Name~n",
             io_lib:format("  ~5s | ~15s | ~15s | ~15s | ~15s ~n",
                           ["Age", "Inserts", "Evictions", "Count", "Alloc"])
             |
             [print_bucket(B) || B <- Buckets]]
    end.

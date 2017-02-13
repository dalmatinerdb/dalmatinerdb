-module(metric_io_cli).
-behavior(clique_handler).

-export([register_cli/0]).

-spec register_cli() -> ok.
register_cli() ->
    %register_cli_usage(),
    register_cli_cfg(),
    register_cli_cmds(),
    register_config_whitelist(),
    ok.

register_cli_cmds() ->
    ok = clique:register_command(["ddb-admin", "io", "config"], [],
                                 node_and_all_flags(), fun io_config/3).

register_cli_cfg() ->
    lists:foreach(
      fun(K) ->
              clique:register_config(K, fun io_cfg_change_callback/3)
      end,
      [["io", "timeout"],
       ["io", "max_async"],
       ["io", "parallel_reads"],
       ["io", "parallel_reads", "min_size"],
       ["io", "parallel_reads", "min_size"],
       ["io", "parallel_reads", "queue_strategy"]]).

node_and_all_flags() ->
    [{node, [{shortname, "n"}, {longname, "node"},
             {typecast, fun clique_typecast:to_node/1}]},
     {all, [{shortname, "a"}, {longname, "all"}]}].


io_config(_CmdBase, _Args, Flags) when length(Flags) > 1 ->
    [clique_status:text("Can't specify both --all and --node flags")];
io_config(_CmdBase, _Args, []) ->
    clique_config:show(config_vars(), []);
io_config(_CmdBase, _Args, [{all, Val}]) ->
    clique_config:show(config_vars(), [{all, Val}]);
io_config(_CmdBase, _Args, [{node, Node}]) ->
    clique_config:show(config_vars(), [{node, Node}]).


config_vars() ->
    ["io.timeout", "io.max_async",
     "io.parallel_reads",
     "io.parallel_reads.min_size",
     "io.parallel_reads.queue_size",
     "io.parallel_reads.queue_strategy"].

io_cfg_change_callback(_, _, _) ->
    metric:update_env().

register_config_whitelist() ->
    ok = clique:register_config_whitelist(
           ["io.max_async",
            "io.timeout",
            "io.parallel_reads",
            "io.parallel_reads.min_size"]).

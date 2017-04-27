-module(dalmatiner_db_cli).
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
    ok = clique:register_command(["ddb-admin", "read_repair", "config"], [],
                                 node_and_all_flags(), fun rr_config/3).

register_cli_cfg() ->
    lists:foreach(
      fun(K) ->
              clique:register_config(K, fun rr_cfg_change_callback/2)
      end,
      [["read_repair", "delay"]]).

node_and_all_flags() ->
    [{node, [{shortname, "n"}, {longname, "node"},
             {typecast, fun clique_typecast:to_node/1}]},
     {all, [{shortname, "a"}, {longname, "all"}]}].


rr_config(_CmdBase, _Args, Flags) when length(Flags) > 1 ->
    [clique_status:text("Can't specify both --all and --node flags")];
rr_config(_CmdBase, _Args, []) ->
    clique_config:show(config_vars(), []);
rr_config(_CmdBase, _Args, [{all, Val}]) ->
    clique_config:show(config_vars(), [{all, Val}]);
rr_config(_CmdBase, _Args, [{node, Node}]) ->
    clique_config:show(config_vars(), [{node, Node}]).


config_vars() ->
    ["read_repair.delay"].

rr_cfg_change_callback(_, _) ->
    metric:update_env().

register_config_whitelist() ->
    ok = clique:register_config_whitelist(["read_repair.delay"]).

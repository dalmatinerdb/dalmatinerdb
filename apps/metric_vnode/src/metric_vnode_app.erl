-module(metric_vnode_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->

    %% Set up metrics reported in this app
    folsom_metrics:new_spiral(metric_vnode_read_repairs),

    case metric_vnode_sup:start_link() of
        {ok, Pid} ->
            ok = riak_core:register([{vnode_module, metric_vnode}]),
            %%ok = riak_core_ring_events:add_guarded_handler(
            %%         metric_ring_event_handler, []),
            %%ok = riak_core_node_watcher_events:add_guarded_handler
            %%         metric_node_event_handler, []),
            ok = riak_core_node_watcher:service_up(metric, self()),
            ok = riak_core_capability:register({ddb, handoff},
                                               [v2, plain],
                                               plain),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.

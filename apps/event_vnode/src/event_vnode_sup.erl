-module(event_vnode_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    VMaster = {event_vnode_master,
               {riak_core_vnode_master, start_link, [event_vnode]},
               permanent, 5000, worker, [riak_core_vnode_master]},
    {ok, {{one_for_one, 5, 10}, [VMaster]}}.


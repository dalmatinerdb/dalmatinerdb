
-module(metric_db_sup).

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
    WriteFSMs = {metric_write_fsm_sup,
                 {metric_write_fsm_sup, start_link, []},
                 permanent, infinity, supervisor, [metric_write_fsm_sup]},

    CoverageFSMs = {metric_coverage_fsm_sup,
                    {metric_coverage_fsm_sup, start_link, []},
                    permanent, infinity, supervisor, [metric_coverage_fsm_sup]},

    ReadFSMs = {metric_read_fsm_sup,
                {metric_read_fsm_sup, start_link, []},
                permanent, infinity, supervisor, [metric_read_fsm_sup]},
    {ok, {{one_for_one, 5, 10}, [WriteFSMs, ReadFSMs, CoverageFSMs]}}.


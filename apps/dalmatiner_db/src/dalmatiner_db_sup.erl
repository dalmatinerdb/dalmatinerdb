-module(dalmatiner_db_sup).

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
    WriteFSMs = {dalmatiner_write_fsm_sup,
                 {dalmatiner_write_fsm_sup, start_link, []},
                 permanent, infinity, supervisor, [dalmatiner_write_fsm_sup]},

    CoverageFSMs = {dalmatiner_coverage_fsm_sup,
                    {dalmatiner_coverage_fsm_sup, start_link, []},
                    permanent, infinity, supervisor, [dalmatiner_coverage_fsm_sup]},

    ReadFSMs = {dalmatiner_read_fsm_sup,
                {dalmatiner_read_fsm_sup, start_link, []},
                permanent, infinity, supervisor, [dalmatiner_read_fsm_sup]},
    UDPListener = {dalmatiner_db_udp_sup,
                   {dalmatiner_db_udp_sup, start_link, []},
                   permanent, infinity, supervisor, [dalmatiner_db_udp_sup]},
    {ok, {{one_for_one, 5, 10}, [WriteFSMs, ReadFSMs, CoverageFSMs, UDPListener]}}.


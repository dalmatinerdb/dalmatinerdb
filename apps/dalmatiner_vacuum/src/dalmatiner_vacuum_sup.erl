%%%-------------------------------------------------------------------
%% @doc dalmatiner_vacuum top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module('dalmatiner_vacuum_sup').

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    Vacuum = {dalmatiner_vacuum,
              {dalmatiner_vacuum, start_link, []},
              permanent, infinity, worker, [dalmatiner_vacuum]},

    {ok, { {one_for_all, 5, 10}, [Vacuum]} }.

%%====================================================================
%% Internal functions
%%====================================================================

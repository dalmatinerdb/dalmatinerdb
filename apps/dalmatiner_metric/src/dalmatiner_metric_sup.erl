%%%-------------------------------------------------------------------
%% @doc dalmatiner_metric top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(dalmatiner_metric_sup).

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
    Metrics = {dalmatiner_metrics,
               {dalmatiner_metrics, start_link, []},
               permanent, infinity, worker, [dalmatiner_metrics]},
    HistogramSup = {ddb_histogram_sup,
                    {ddb_histogram_sup, start_link, []},
                    permanent, infinity, supervisor, [ddb_histogram_sup]},
    {ok, { {one_for_all, 0, 1},
           [HistogramSup, Metrics]} }.

%%====================================================================
%% Internal functions
%%====================================================================

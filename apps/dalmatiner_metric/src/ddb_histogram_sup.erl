%% @doc Supervise the rts_write FSM.
-module(ddb_histogram_sup).
-behavior(supervisor).

-export([start_histogram/1,
         start_link/0]).

-export([init/1]).

-ignore_xref([init/1,
              start_link/0]).

start_histogram(Args) ->
    supervisor:start_child(?MODULE, Args).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ddb_histogram:init(),
    ReadFsm =
        {undefined,
         {ddb_histogram, start_link, []},
         temporary, 5000, worker, [ddb_histogram]},
    {ok, {{simple_one_for_one, 10, 10}, [ReadFsm]}}.

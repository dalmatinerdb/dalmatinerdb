%% @doc Supervise the rts_write FSM.
-module(dalmatiner_read_fsm_sup).
-behavior(supervisor).

-export([start_read_fsm/1,
         start_link/0]).

-export([init/1]).

-ignore_xref([init/1,
	      start_link/0]).

start_read_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ReadFsm =
        {undefined,
         {dalmatiner_read_fsm, start_link, []},
         temporary, 5000, worker, [dalmatiner_read_fsm]},
    {ok, {{simple_one_for_one, 10, 10}, [ReadFsm]}}.

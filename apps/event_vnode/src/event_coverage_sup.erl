%% @doc Supervise the rts_write FSM.
-module(event_coverage_sup).

-behavior(supervisor).

-export([start_coverage/2,
         start_coverage/3,
         start_link/0]).

-export([init/1]).

-ignore_xref([init/1,
              start_link/0,
              start_coverage/2,
              start_coverage/3]).

start_coverage(Module, From, Args) ->
    supervisor:start_child(?MODULE, [Module, From, Args]).

start_coverage(From, Args) ->
    supervisor:start_child(?MODULE, [event_coverage, From, Args]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ReadFsm = {undefined,
               {riak_core_coverage_fsm, start_link, []},
               temporary, 5000, worker, [riak_core_coverage_fsm]},
    {ok, {{simple_one_for_one, 10, 10}, [ReadFsm]}}.

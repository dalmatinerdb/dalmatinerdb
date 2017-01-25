-module(metric_coverage).

-behaviour(riak_core_coverage_fsm).

-export([
         init/2,
         process_results/2,
         finish/2,
         start/1
        ]).

-ignore_xref([
         start/1
        ]).


-record(state, {replies, r, reqid, from}).

start(Request) ->
    ReqID = mk_reqid(),
    metric_coverage_sup:start_coverage(
      ?MODULE, {self(), ReqID, something_else},
      Request),
    receive
        ok ->
            ok;
        {ok, Result} ->
            {ok, Result}
    after 60000 ->
            {error, timeout}
    end.

%% The first is the vnode service used
init({From, ReqID, _}, Request) ->
    {ok, NVal} = application:get_env(dalmatiner_db, n),
    {ok, R} = application:get_env(dalmatiner_db, r),
    %% all - full coverage; allup - partial coverage
    VNodeSelector = allup,
    %% Same as R value here, TODO: Make this dynamic
    PrimaryVNodeCoverage = R,
    %% We timeout after 5s
    Timeout = 55000,
    State = #state{replies = btrie:new(), r = R,
                   from = From, reqid = ReqID},
    {Request, VNodeSelector, NVal, PrimaryVNodeCoverage,
     metric, metric_vnode_master, Timeout, riak_core_coverage_plan, State}.

process_results({ok, _ReqID, _IdxNode, Metrics},
                State = #state{replies = Replies}) ->
    Replies1 = btrie:merge(fun(_, _, _) -> t end, Replies, Metrics),
    {done, State#state{replies = Replies1}};

process_results(Result, State) ->
    lager:error("Unknown process results call: ~p ~p", [Result, State]),
    {done, State}.

finish(clean, State = #state{replies = Replies,
                             from = From}) ->
    From ! {ok, btrie:fetch_keys(Replies)},
    {stop, normal, State};

finish(How, State) ->
    lager:error("Unknown process results call: ~p ~p", [How, State]),
    {error, failed}.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

mk_reqid() ->
    erlang:unique_integer().

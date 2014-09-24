-module(metric_worker).
-behaviour(riak_core_vnode_worker).

-export([init_worker/3,
         handle_work/3]).

-record(state, {index}).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Initialize the worker. Currently only the VNode index
%% parameter is used.
init_worker(VNodeIndex, _Args, _Props) ->
    {ok, #state{index=VNodeIndex}}.

%% @doc Perform the asynchronous fold operation.
handle_work({fold, FoldFun, FinishFun}, _Sender, State) ->
    try
        FinishFun(FoldFun())
    catch
        receiver_down -> ok;
        stop_fold -> ok
    end,
    {noreply, State}.

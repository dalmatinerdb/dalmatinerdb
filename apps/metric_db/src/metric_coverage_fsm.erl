%% @doc The coordinator for stat get operations.  The key here is to
%% generate the preflist just like in wrtie_fsm and then query each
%% replica and wait until a quorum is met.
-module(metric_coverage_fsm).
-behavior(gen_fsm).

-define(DEFAULT_TIMEOUT, 5000).
-define(N, 1).
-define(R, 1).
-define(W, 1).

%% API
-export([start_link/6, start/2, start/3, start/4]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2, waiting/2]).

-ignore_xref([
              code_change/4,
              execute/2,
              handle_event/3,
              handle_info/3,
              handle_sync_event/4,
              init/1,
              prepare/2,
              start_link/6,
              terminate/3,
              waiting/2,
              start/3,
              start/2,
              start/4
             ]).

-record(state, {req_id,
                from,
                entity,
                op,
                r,
                n,
                preflist,
                start,
                num_r=0,
                size,
                timeout=?DEFAULT_TIMEOUT,
                val,
                vnode,
                system,
                replies=[]}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqID, {VNode, System}, Op, From, Entity, Val) ->
    gen_fsm:start_link(?MODULE, [ReqID, {VNode, System}, Op, From, Entity, Val], []).

start(VNodeInfo, Op) ->
    start(VNodeInfo, Op, undefined).

start(VNodeInfo, Op, User) ->
    start(VNodeInfo, Op, User, undefined).

start(VNodeInfo, Op, User, Val) ->
    ReqID = mk_reqid(),
    metric_coverage_fsm_sup:start_read_fsm(
      [ReqID, VNodeInfo, Op, self(), User, Val]
     ),
    receive
        {ReqID, ok} ->
            ok;
        {ReqID, ok, Result} ->
            {ok, Result}
    after ?DEFAULT_TIMEOUT ->
            {error, timeout}
    end.

%%%===================================================================
%%% States
%%%===================================================================

%% Intiailize state data.
init([ReqId, {VNode, System}, Op, From]) ->
    init([ReqId, {VNode, System}, Op, From, undefined, undefined]);

init([ReqId, {VNode, System}, Op, From, Entity]) ->
    init([ReqId, {VNode, System}, Op, From, Entity, undefined]);

init([ReqId, {VNode, System}, Op, From, Entity, Val]) ->
    {N, R, _W} = case application:get_key(System) of
                     {ok, Res} ->
                         Res;
                     undefined ->
                         {?N, ?R, ?W}
                 end,
    SD = #state{req_id=ReqId,
                from=From,
                op=Op,
                val=Val,
                r=R,
                n=N,
                start=now(),
                vnode=VNode,
                system=System,
                entity=Entity},
    {ok, prepare, SD, 0}.

%% @doc Calculate the Preflist.
prepare(timeout, SD0=#state{system=System,
                            n=N,
                            req_id=ReqId}) ->
    PVC = N,
    {Nodes, _Other} =
        riak_core_coverage_plan:create_plan(
          all, N, PVC, ReqId, System),
    {ok, CHash} = riak_core_ring_manager:get_my_ring(),
    {Num, _} = riak_core_ring:chash(CHash),
    SD = SD0#state{preflist=Nodes, size=Num},
    {next_state, execute, SD, 0}.

%% @doc Execute the get reqs.
execute(timeout, SD0=#state{req_id=ReqId,
                            entity=Entity,
                            op=Op,
                            val=Val,
                            vnode=VNode,
                            preflist=Prelist}) ->
    case Entity of
        undefined ->
            VNode:Op(Prelist, ReqId);
        _ ->
            case Val of
                undefined ->
                    VNode:Op(Prelist, ReqId, Entity);
                _ ->

                    VNode:Op(Prelist, ReqId, Entity, Val)
            end
    end,
    {next_state, waiting, SD0}.

%% Waiting for returns from coverage replies

waiting({
         {undefined,{_Partition, _Node} = IdxNode},
	 {ok,ReqID,IdxNode,Obj}},
	SD0=#state{num_r = NumR0, size=Size, from=From, replies=Replies0, r=R}) ->
    NumR = NumR0 + 1,
    Replies1 = case Replies0 of
		   [] ->
		       dict:new();
		   _ ->
		       Replies0
	       end,
    Replies = lists:foldl(fun (Key, D) ->
				   dict:update_counter(Key, 1, D)
			   end, Replies1, Obj),
    SD = SD0#state{num_r=NumR,replies=Replies},
    if
        NumR =:= Size ->
	    MergedReplies = dict:fold(fun(_Key, Count, Keys) when Count < R->
					      Keys;
					 (Key, _Count, Keys) ->
					      [Key | Keys]
				      end, [], Replies),
	    From ! {ReqID, ok, MergedReplies},
	    {stop, normal, SD};
        true ->
	    {next_state, waiting, SD}
    end.

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

mk_reqid() ->
    erlang:phash2(erlang:now()).

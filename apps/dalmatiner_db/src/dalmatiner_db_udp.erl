%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 13 Jun 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(dalmatiner_db_udp).

-behaviour(gen_server).
-include_lib("mmath/include/mmath.hrl").
-include_lib("dproto/include/dproto.hrl").
-include_lib("mstore/include/mstore.hrl").

-define(DT_DDB_UDP_SIZE, 4501).
-define(DT_DDB_UDP_CNT, 4502).
-define(DT_DDB_UDP_LOOP, 4503).

%% API
-export([start_link/1]).

-ignore_xref([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {sock, port, recbuf, cbin, nodes, n, w, fast_loop_count, wait}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Port) ->
    gen_server:start_link(?MODULE, [Port], []).

loop(N) ->
    loop(self(), N).

loop(Pid, N) ->
    gen_server:cast(Pid, {loop, N}).
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Port]) ->
    {ok, RB} = application:get_env(dalmatiner_db, udp_buffer),
    {ok, FLC} = application:get_env(dalmatiner_db, fast_loop_count),
    {ok, Wait} = application:get_env(dalmatiner_db, loop_wait),
    {ok, N} = application:get_env(dalmatiner_db, n),
    {ok, W} = application:get_env(dalmatiner_db, w),
    {ok, Sock} = gen_udp:open(Port, [binary, {active, false}, {recbuf, RB}]),
    loop(0),
    {ok, #state{sock=Sock, port=Port, recbuf=RB, n=N, w=W, fast_loop_count=FLC,
                wait=Wait}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({loop, 0}, State) ->
    dyntrace:p(?DT_DDB_UDP_LOOP, State#state.port),
    loop(State#state.fast_loop_count),
    {ok, CBin} = riak_core_ring_manager:get_chash_bin(),
    Nodes = chash:nodes(chashbin:to_chash(CBin)),
    Nodes1 = [{I, riak_core_apl:get_apl(I, State#state.n, metric)} || {I, _} <- Nodes],
    {noreply, State#state{cbin=CBin, nodes=orddict:from_list(Nodes1)}};

handle_cast({loop, N}, State = #state{sock=S, cbin=CBin, nodes=Nodes, w=W,
                                      port=LPort}) ->
    case gen_udp:recv(S, State#state.recbuf, State#state.wait) of
        {ok, {_Address, _Port,
              <<0,
                _BS:?BUCKET_SS/integer, Bucket:_BS/binary,
                D/binary>>}} ->
            dyntrace:p(?DT_DDB_UDP_SIZE, LPort, byte_size(D)),
            case handle_data(D, Bucket, W, LPort, CBin, Nodes, 0, dict:new()) of
                ok ->
                    handle_cast({loop, N-1}, State);
                E ->
                    lager:error("[udp] Fast loop cancled because of: ~p.", [E]),
                    handle_cast({loop, 0}, State),
                    {noreply, State}

            end;
        _ ->
            handle_cast({loop, 0}, State),
            {noreply, State}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_data(<<T:?TIME_SIZE/integer,
              _MS:?METRIC_SS/integer, Metric:_MS/binary,
              _DS:?DATA_SS/integer, Data:_DS/binary,
              R/binary>>,
            Bucket, W, LPort, CBin, Nodes, Cnt, Acc) when (_DS rem ?DATA_SIZE) == 0 ->
    DocIdx = riak_core_util:chash_key({Bucket, Metric}),
    {Idx, _} = chashbin:itr_value(chashbin:exact_iterator(DocIdx, CBin)),
    Acc1 = dict:append(Idx, {Bucket, Metric, T, Data}, Acc),
    handle_data(R, Bucket, W, LPort, CBin, Nodes, Cnt + 1, Acc1);
handle_data(<<>>, _Bucket, W, LPort, _, Nodes, Cnt, Acc) ->
    dyntrace:p(?DT_DDB_UDP_CNT, LPort, Cnt),
    metric:mput(Nodes, Acc, W);
handle_data(R, _Bucket, W, LPort, _, Nodes, Cnt, Acc) ->
    dyntrace:p(?DT_DDB_UDP_CNT, LPort, Cnt),
    lager:error("[udp] unknown content: ~p", [R]),
    metric:mput(Nodes, Acc, W).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, #state{sock = S}) ->
    gen_udp:close(S),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

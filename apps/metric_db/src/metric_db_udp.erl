%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 13 Jun 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(metric_db_udp).

-behaviour(gen_server).

-include_lib("mstore/include/mstore.hrl").

%% API
-export([start_link/1]).

-ignore_xref([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(FAST_LOOP_CNT, 10000).

-record(state, {sock, port, recbuf, cbin}).

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
    RB = case application:get_env(metric_db, udp_buffer) of
             {ok, V} ->
                 V;
             _ ->
                 32768
         end,
    {ok, Sock} = gen_udp:open(Port, [binary, {active, false}, {recbuf, RB}]),
    loop(?FAST_LOOP_CNT),
    {ok, CBin} = riak_core_ring_manager:get_chash_bin(),
    {ok, #state{sock=Sock, port=Port, recbuf=RB, cbin=CBin}}.

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
    loop(?FAST_LOOP_CNT),
    {ok, CBin} = riak_core_ring_manager:get_chash_bin(),
    {noreply, State#state{cbin=CBin}};

handle_cast({loop, N}, State = #state{sock=S, cbin=CBin}) ->
    case gen_udp:recv(S, State#state.recbuf, 100) of
        {ok, {_Address, _Port, D}} ->
            handle_data(D, CBin, dict:new()),
            handle_cast({loop, N-1}, State);
        _ ->
            loop(?FAST_LOOP_CNT),
            {noreply, State}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_data(<<0, T:64/integer, L:16/integer, Metric:L/binary, S:16/integer,
              Data:S/binary, R/binary>>, CBin, Acc) when (S rem ?DATA_SIZE) == 0 ->
    DocIdx = riak_core_util:chash_key({<<"metric">>, Metric}),
    {Idx, _} = chashbin:itr_value(chashbin:exact_iterator(DocIdx, CBin)),
    Acc1 = dict:append(Idx, {Metric, T, Data}, Acc),
    handle_data(R, CBin, Acc1);
handle_data(_, _, Acc) ->
    metric:mput(Acc).

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

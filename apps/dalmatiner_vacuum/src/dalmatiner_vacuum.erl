%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2015, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created :  4 Aug 2015 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(dalmatiner_vacuum).

-behaviour(gen_server).

%% API
-export([start_link/0, register/0]).
-ignore_xref([start_link/0]).


%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(TICK, 1000*60*60).
-record(state,
        {
          vnodes = queue:new() :: queue:queue(),
          tick = ?TICK
        }).

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
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register() ->
    gen_server:call(?SERVER, {register, self()}).

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
init([]) ->
    Tick = case application:get_env(dalmatiner_vaccum, interval) of
                   {ok, FS} ->
                   FS;
               _ ->
                   ?TICK
           end,
    erlang:send_after(Tick, self(), vacuum),
    {ok, #state{tick = Tick}}.

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
handle_call({register, VNode}, _From, State = #state{vnodes = VNodes}) ->
    lager:info("[vacuum] Adding vnode ~p.", [VNode]),
    Ref = erlang:monitor(process, VNode),
    VNodes1 = queue:in({Ref, VNode}, VNodes),
    {reply, ok, State#state{vnodes = VNodes1}};

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

handle_cast(_Msg, State) ->
    {noreply, State}.

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
handle_info({'DOWN', Ref, process, VNode, Reason},
            State = #state{vnodes = VNodes}) ->
    lager:info("[vacuum] Removing node ~p for reason: ~p", [VNode, Reason]),
    VNodes1 = queue:filter(fun (_D) when _D =:= {Ref, VNode} -> false;
                               (_) -> true
                           end, VNodes),
    {noreply, State#state{vnodes = VNodes1}};

handle_info(vacuum, State = #state{vnodes = VNodes}) ->
    VNodes2 = case queue:out(VNodes) of
                  {{value, VNode = {_, Pid}}, VNodes1} ->
                      Pid ! vacuum,
                      queue:in(VNode, VNodes1);
                  {empty, VNodes1} ->
                      VNodes1
              end,
    erlang:send_after(?TICK, self(), vacuum),
    {noreply, State#state{vnodes = VNodes2}};


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
terminate(_Reason, _State) ->
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

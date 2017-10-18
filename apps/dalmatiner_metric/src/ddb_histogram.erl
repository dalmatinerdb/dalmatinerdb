%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2017, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 12 Feb 2017 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(ddb_histogram).

-behaviour(gen_server).

%% API
-export([start_link/1, init/0, register/1, timed_update/2, timed_update/4,
         get/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ignore_xref([start_link/1]).

-define(TBL, ?MODULE).

-record(state, {name, hist}).



%%%===================================================================
%%% API
%%%===================================================================

init() ->
    ets:new(?TBL, [named_table, set, public]).

register(Name) ->
    ddb_histogram_sup:start_histogram([Name]).

%% get(Name) ->
%%     case ets:lookup(?TBL, Name) of
%%         [{Name, Pid}] ->
%%             gen_server:call(Pid, get);
%%         _ ->
%%             {error, not_found}
%%     end.

get() ->
    [{Name, gen_server:call(Pid, get)} || {Name, Pid} <- ets:tab2list(?TBL)].

timed_update(Name, Fun) ->
    {Time, Result} = timer:tc(Fun),
    notify(Name, Time),
    Result.

timed_update(Name, Mod, Fun, Args) ->
    {Time, Result} = timer:tc(Mod, Fun, Args),
    notify(Name, Time),
    Result.

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Name) ->
    case gen_server:start_link(?MODULE, [Name], []) of
        {ok, Pid} ->
            ets:insert(?TBL, {Name, Pid}),
            {ok, Pid};
        Error ->
            Error
    end.

notify(Name, Value) ->
    case ets:lookup(?TBL, Name) of
        [{Name, Pid}] ->
            gen_server:cast(Pid, {notify, Value});
        _ ->
            {error, not_found}
    end.

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
init([Name]) ->
    process_flag(trap_exit, true),
    %% We do exclude everything over an hour because seriously, wtf!
    {ok, #state{name = Name, hist = new()}}.

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
handle_call(get, _From, State = #state{hist = H}) ->
    Reply = [
             {<<"max">>,    hdr_histogram:max(H)},
             {<<"mean">>,   hdr_histogram:mean(H)},
             {<<"median">>, hdr_histogram:median(H)},
             {<<"min">>,    hdr_histogram:min(H)},
             {<<"p50">>,    hdr_histogram:percentile(H, 50.0)},
             {<<"p75">>,    hdr_histogram:percentile(H, 75.0)},
             {<<"p90">>,    hdr_histogram:percentile(H, 90.0)},
             {<<"p95">>,    hdr_histogram:percentile(H, 95.0)},
             {<<"p99">>,    hdr_histogram:percentile(H, 99.0)},
             {<<"p999">>,   hdr_histogram:percentile(H, 99.9)},
             {<<"p9999">>,  hdr_histogram:percentile(H, 99.99)},
             {<<"count">>,  hdr_histogram:get_total_count(H)}
            ],
    hdr_histogram:close(H),
    {reply, Reply, State#state{hist = new()}}.

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
handle_cast({notify, V}, State) ->
    hdr_histogram:record(State#state.hist, V),
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

new() ->
    MaxValue =  3600000000,
    %% 3 Significant values should be enough
    SignificantDigest = 3,
    {ok, Hist} = hdr_histogram:open(MaxValue, SignificantDigest),
    Hist.

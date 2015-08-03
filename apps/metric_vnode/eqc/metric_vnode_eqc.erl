-module(metric_vnode_eqc).

-include_lib("riak_core/include/riak_core_vnode.hrl").

-include_lib("mmath/include/mmath.hrl").
-include_lib("eqc/include/eqc.hrl").

-compile(export_all).

-define(DIR, ".qcdata").
-define(T, gb_trees).
-define(V, metric_vnode).
-define(B, <<"bucket">>).
-define(M, <<"metric">>).

%%%-------------------------------------------------------------------
%%% Generators
%%%-------------------------------------------------------------------

non_z_int() ->
    ?SUCHTHAT(I, int(), I =/= 0).

offset() ->
    choose(0, 5000).

tree_set(Tr, _T, []) ->
    Tr;

tree_set(Tr, T, [V | R]) ->
    Tr1 = ?T:enter(T, V, Tr),
    tree_set(Tr1, T+1, R).

new() ->
    {ok, S, _} = ?V:init([0]),
    T = ?T:empty(),
    {S, T}.

repair({S, Tr}, T, Vs) ->
    case overlap(Tr, T, Vs) of
        [] ->
            Command = {repair, ?B, ?M, T, mmath_bin:from_list(Vs)},
            {noreply, S1} = ?V:handle_command(Command, sender, S),
            Tr1 = tree_set(Tr, T, Vs),
            {S1, Tr1};
        _ ->
            {S, Tr}
    end.

put({S, Tr}, T, Vs) ->
    case overlap(Tr, T, Vs) of
        [] ->
            Command = {put, ?B, ?M, {T, mmath_bin:from_list(Vs)}},
            {reply, ok, S1} = ?V:handle_command(Command, sender, S),
            Tr1 = tree_set(Tr, T, Vs),
            {S1, Tr1};
        _ ->
            {S, Tr}
    end.

mput({S, Tr}, T, Vs) ->
    case overlap(Tr, T, Vs) of
        [] ->
            Command = {mput, [{?B, ?M, T, mmath_bin:from_list(Vs)}]},
            {reply, ok, S1} = ?V:handle_command(Command, sender, S),
            Tr1 = tree_set(Tr, T, Vs),
            {S1, Tr1};
        _ ->
            {S, Tr}
    end.

overlap(Tr, Start, Vs) ->
    End = Start + length(Vs),
    [T || {T, _} <- ?T:to_list(Tr),
          T >= Start, T =< End].

get(S, T, C) ->
    ReqID = T,
    Command = {get, ReqID, ?B, ?M, {T, C}},
    case ?V:handle_command(Command, {raw, ReqID, self()}, S) of
        {noreply, _S1} ->
            receive
                {ReqID, {ok, ReqID, _, D}} ->
                    D
            after
                1000 ->
                    timeout
            end;
        {reply, {ok, _, _, Reply}, _S1} ->
            Reply;
        {reply, {ok, Reply}, _S1} ->
            Reply
    end.

vnode() ->
    ?SIZED(Size,vnode(Size)).

non_empty_list(T) ->
    ?SUCHTHAT(L, list(T), L =/= []).

values() ->
	{offset(), non_empty_list(non_z_int())}.

vnode(Size) ->
    ?LAZY(oneof(
            [{call, ?MODULE, new, []} || Size == 0]
            ++ [?LETSHRINK(
                   [V], [vnode(Size-1)],
                   ?LET({T, Vs}, values(),
                        oneof(
                          [{call, ?MODULE, put, [V, T, Vs]},
                           {call, ?MODULE, mput, [V, T, Vs]},
                           {call, ?MODULE, repair, [V, T, Vs]}])))  || Size > 0])).
%%%-------------------------------------------------------------------
%%% Properties
%%%-------------------------------------------------------------------

prop_gb_comp() ->
    ?FORALL(D, vnode(),
            begin
                os:cmd("rm -r data"),
                os:cmd("mkdir data"),
                {S, T} = eval(D),
                List = ?T:to_list(T),
                List1 = [{get(S, Time, 1), V} || {Time, V} <- List],
                List2 = [{unlist(mmath_bin:to_list(Vs)), Vt} || {{_ ,Vs}, Vt} <- List1],
                List3 = [true || {_V, _V} <- List2],
                Len = length(List),
                ?V:terminate(normal, S),
                ?WHENFAIL(io:format(user,
                                    "L : ~p~n"
                                    "L1: ~p~n"
                                    "L2: ~p~n"
                                    "L3: ~p~n", [List, List1, List2, List3]),
                          length(List1) == Len andalso
                          length(List2) == Len andalso
                          length(List3) == Len)
            end
           ).

prop_is_empty() ->
    ?FORALL(D, vnode(),
            begin
                os:cmd("rm -r data"),
                os:cmd("mkdir data"),
                {S, T} = eval(D),
                {Empty, _S1} = ?V:is_empty(S),
                TreeEmpty = ?T:is_empty(T),
                if
                    Empty == TreeEmpty ->
                        ok;
                    true ->
                        io:format(user, "~p == ~p~n", [S, T])
                end,
                ?V:terminate(normal, S),
                ?WHENFAIL(io:format(user, "L: ~p /= ~p~n", [Empty, TreeEmpty]),
                          Empty == TreeEmpty)
            end).

prop_empty_after_delete() ->
    ?FORALL(D, vnode(),
            begin
                os:cmd("rm -r data"),
                os:cmd("mkdir data"),
                {S, _T} = eval(D),
                {ok, S1} = ?V:delete(S),
                {Empty, _S3} = ?V:is_empty(S1),
                ?V:terminate(normal, S),
                Empty == true
            end).

prop_handoff() ->
    ?FORALL(D, vnode(),
            begin
                os:cmd("rm -r data"),
                os:cmd("mkdir data"),
                {S, T} = eval(D),

                List = ?T:to_list(T),

                List1 = [{get(S, Time, 1), V} || {Time, V} <- List],
                List2 = [{unlist(mmath_bin:to_list(Vs)), Vt} || {{_ ,Vs}, Vt} <- List1],
                List3 = [true || {_V, _V} <- List2],

                Fun = fun(K, V, A) ->
                              [?V:encode_handoff_item(K, V) | A]
                      end,
                FR = ?FOLD_REQ{foldfun=Fun, acc0=[]},
                {async,{fold, AsyncH, _}, _, S1} =
                    ?V:handle_handoff_command(FR, self(), S),
                L = AsyncH(),
                L1 = lists:sort(L),
                {ok, C, _} = ?V:init([1]),
                C1 = lists:foldl(fun(Data, SAcc) ->
                                         {reply, ok, SAcc1} = ?V:handle_handoff_data(Data, SAcc),
                                         SAcc1
                                 end, C, L1),

                List4 = [{get(C1, Time, 1), V} || {Time, V} <- List],
                List5 = [{unlist(mmath_bin:to_list(Vs)), Vt} || {{_ ,Vs}, Vt} <- List1],
                List6 = [true || {_V, _V} <- List2],

                {async,{fold, AsyncHc, _}, _, C2} =
                    ?V:handle_handoff_command(FR, self(), C1),
                Lc = AsyncHc(),
                Lc1 = lists:sort(Lc),
                {async,{fold, Async, _}, _, _} =
                    ?V:handle_coverage({metrics, ?B}, all, self(), S1),
                Ms = Async(),
                {async,{fold, AsyncC, _}, _, _} =
                    ?V:handle_coverage({metrics, ?B}, all, self(), C2),
                MsC = AsyncC(),

                Len = length(List),


                ?V:terminate(normal, S1),
                ?V:terminate(normal, C2),

                ?WHENFAIL(io:format(user, "L: ~p /= ~p~n"
                                    "M: ~p /= ~p~n",
                                    [Lc1, L1, gb_sets:to_list(MsC),
                                     gb_sets:to_list(Ms)]),
                          Lc1 == L1 andalso
                          btrie:fetch_keys(MsC) == btrie:fetch_keys(Ms) andalso
                          length(List1) == Len andalso
                          length(List2) == Len andalso
                          length(List3) == Len andalso
                          length(List4) == Len andalso
                          length(List5) == Len andalso
                          length(List6) == Len)
            end).


%%%-------------------------------------------------------------------
%%% Helper
%%%-------------------------------------------------------------------

unlist([E]) ->
    E.

setup() ->
    meck:new(riak_core_metadata, [passthrough]),
    meck:expect(riak_core_metadata, get, fun(_, _) -> undefined end),
    meck:expect(riak_core_metadata, put, fun(_, _, _) -> ok end),
    meck:new(dalmatiner_opt, [passthrough]),
    meck:expect(dalmatiner_opt, get, fun( _, _, _, _, Dflt) -> Dflt end),
    ok.

cleanup(_) ->
    meck:unload(riak_core_metadata),
    meck:unload(dalmatiner_opt),
    ok.

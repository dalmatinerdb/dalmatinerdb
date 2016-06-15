-module(metric_vnode_eqc).

-include_lib("riak_core/include/riak_core_vnode.hrl").
-include_lib("mmath/include/mmath.hrl").
-include_lib("eqc/include/eqc.hrl").

-compile(export_all).

-define(DIR, ".qcdata").
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
    Tr1 = gb_trees:enter(T, V, Tr),
    tree_set(Tr1, T+1, R).

new() ->
    {ok, S, _} = metric_vnode:init([0]),
    T = gb_trees:empty(),
    {S, T}.

repair({S, Tr}, T, Vs) ->
    case overlap(Tr, T, Vs) of
        [] ->
            Command = {repair, ?B, ?M, T, mmath_bin:from_list(Vs)},
            {noreply, S1} = metric_vnode:handle_command(Command, sender, S),
            Tr1 = tree_set(Tr, T, Vs),
            {S1, Tr1};
        _ ->
            {S, Tr}
    end.

put({S, Tr}, T, Vs) ->
    case overlap(Tr, T, Vs) of
        [] ->
            Command = {put, ?B, ?M, {T, mmath_bin:from_list(Vs)}},
            {reply, ok, S1} = metric_vnode:handle_command(Command, sender, S),
            Tr1 = tree_set(Tr, T, Vs),
            {S1, Tr1};
        _ ->
            {S, Tr}
    end.

mput({S, Tr}, T, Vs) ->
    case overlap(Tr, T, Vs) of
        [] ->
            Command = {mput, [{?B, ?M, T, mmath_bin:from_list(Vs)}]},
            {reply, ok, S1} = metric_vnode:handle_command(Command, sender, S),
            Tr1 = tree_set(Tr, T, Vs),
            {S1, Tr1};
        _ ->
            {S, Tr}
    end.

overlap(Tr, Start, Vs) ->
    End = Start + length(Vs),
    [T || {T, _} <- gb_trees:to_list(Tr),
          T >= Start, T =< End].

get(S, T, C) ->
    ReqID = T,
    ReplyNode = nonode@nohost,
    Command = {get, ReqID, ?B, ?M, {T, C}},
    case metric_vnode:handle_command(Command, {raw, ReqID, self()}, S) of
        {noreply, _S1} ->
            receive
                {ReqID, {ok, ReqID, {_Partition, ReplyNode}, D}} ->
                    D
            after
                1000 ->
                    timeout
            end;
        {reply, {ok, _, {_Partition, ReplyNode}, Reply}, _S1} ->
            Reply
    end.

vnode() ->
    ?SIZED(Size, vnode(Size)).

non_empty_list(T) ->
    ?SUCHTHAT(L, list(T), L =/= []).

values() ->
    {offset(), non_empty_list(non_z_int())}.

vnode(Size) ->
    S = Size,
    ?LAZY(oneof(
            [{call, ?MODULE, new, []} || S == 0]
            ++ [?LETSHRINK(
                   [V], [vnode(S-1)],
                   ?LET({T, Vs}, values(),
                        oneof(
                          [{call, ?MODULE, put, [V, T, Vs]},
                           {call, ?MODULE, mput, [V, T, Vs]},
                           {call, ?MODULE, repair, [V, T, Vs]}]))) || S > 0])).
%%%-------------------------------------------------------------------
%%% Properties
%%%-------------------------------------------------------------------

prop_gb_comp() ->
    ?SETUP(
       fun setup_fn/0,
       ?FORALL(D, vnode(),
               begin
                   os:cmd("rm -r data"),
                   os:cmd("mkdir data"),
                   {S, T} = eval(D),
                   List = gb_trees:to_list(T),
                   List1 = [{get(S, Time, 1), V} || {Time, V} <- List],
                   List2 = [{unlist(Vs), Vt} || {{_ , Vs}, Vt} <- List1],
                   List3 = [true || {_V, _V} <- List2],
                   Len = length(List),
                   metric_vnode:terminate(normal, S),
                   ?WHENFAIL(io:format(user,
                                       "L : ~p~n"
                                       "L1: ~p~n"
                                       "L2: ~p~n"
                                       "L3: ~p~n", [List, List1, List2, List3]),
                             length(List1) == Len andalso
                             length(List2) == Len andalso
                             length(List3) == Len)
               end)).

prop_is_empty() ->
    ?SETUP(
       fun setup_fn/0,
       ?FORALL(D, vnode(),
               begin
                   os:cmd("rm -r data"),
                   os:cmd("mkdir data"),
                   {S, T} = eval(D),
                   Empty = case metric_vnode:is_empty(S) of
                               {Res, _S1} -> Res;
                               {Res, _Cnt, _S1} -> Res
                           end,
                   TreeEmpty = gb_trees:is_empty(T),
                   if
                       Empty == TreeEmpty ->
                           ok;
                       true ->
                           io:format(user, "~p == ~p~n", [S, T])
                   end,
                   metric_vnode:terminate(normal, S),
                   ?WHENFAIL(io:format(user, "L: ~p /= ~p~n",
                                       [Empty, TreeEmpty]), Empty == TreeEmpty)
               end)).

prop_empty_after_delete() ->
    ?SETUP(
       fun setup_fn/0,
       ?FORALL(D, vnode(),
               begin
                   os:cmd("rm -r data"),
                   os:cmd("mkdir data"),
                   {S, _T} = eval(D),
                   {ok, S1} = metric_vnode:delete(S),
                   Empty = case metric_vnode:is_empty(S1) of
                               {Res, _S2} -> Res;
                               {Res, _Cnt, _S2} -> Res
                           end,
                   metric_vnode:terminate(normal, S),
                   Empty == true
               end)).

prop_handoff() ->
    ?SETUP(
       fun setup_fn/0,
       ?FORALL(D, vnode(),
               begin
                   os:cmd("rm -r data"),
                   os:cmd("mkdir data"),
                   {S, T} = eval(D),

                   List = gb_trees:to_list(T),

                   List1 = [{get(S, Time, 1), V} || {Time, V} <- List],
                   List2 = [{unlist(Vs), Vt} || {{_ , Vs}, Vt} <- List1],
                   List3 = [true || {_V, _V} <- List2],

                   Fun = fun(K, V, A) ->
                                 [metric_vnode:encode_handoff_item(K, V) | A]
                         end,
                   FR = ?FOLD_REQ{foldfun=Fun, acc0=[]},
                   {async, {fold, AsyncH, _}, _, S1} =
                       metric_vnode:handle_handoff_command(FR, self(), S),
                   L = AsyncH(),
                   L1 = lists:sort(L),
                   {ok, C, _} = metric_vnode:init([1]),
                   C1 = lists:foldl(fun(Data, SAcc) ->
                                            {reply, ok, SAcc1} =
                                                handoff_recv(Data, SAcc),
                                            SAcc1
                                    end, C, L1),

                   List4 = [{get(C1, Time, 1), V} || {Time, V} <- List],
                   List5 = [{unlist(Vs), Vt} || {{_ , Vs}, Vt} <- List1],
                   List6 = [true || {_V, _V} <- List2],

                   {async, {fold, AsyncHc, _}, _, C2} =
                       metric_vnode:handle_handoff_command(FR, self(), C1),
                   Lc = AsyncHc(),
                   Lc1 = lists:sort(Lc),
                   {async, {fold, Async, _}, _, _} =
                       metric_vnode:handle_coverage({metrics, ?B},
                                                    all, self(), S1),
                   Ms = Async(),
                   {async, {fold, AsyncC, _}, _, _} =
                       metric_vnode:handle_coverage({metrics, ?B},
                                                    all, self(), C2),
                   MsC = AsyncC(),

                   Len = length(List),


                   metric_vnode:terminate(normal, S1),
                   metric_vnode:terminate(normal, C2),

                   ?WHENFAIL(io:format(user, "L: ~p /= ~p~n"
                                       "M: ~p /= ~p~n",
                                       [Lc1, L1, gb_sets:to_list(MsC),
                                        gb_sets:to_list(Ms)]),
                             Lc1 == L1 andalso
                             fetch_keys(MsC) == fetch_keys(Ms) andalso
                             length(List1) == Len andalso
                             length(List2) == Len andalso
                             length(List3) == Len andalso
                             length(List4) == Len andalso
                             length(List5) == Len andalso
                             length(List6) == Len)
               end)).


%%%-------------------------------------------------------------------
%%% Helper
%%%-------------------------------------------------------------------

unlist(Vs) ->
    [Inner] = mmath_bin:to_list(Vs),
    Inner.

fetch_keys(T) ->
    btrie:fetch_keys(T).

handoff_recv(Data, SAcc) ->
    metric_vnode:handle_handoff_data(Data, SAcc).

setup_fn() ->
    setup(),
    fun cleanup/0.

setup() ->
    meck:new(riak_core_metadata, [passthrough]),
    meck:expect(riak_core_metadata, get, fun(_, _) -> undefined end),
    meck:expect(riak_core_metadata, put, fun(_, _, _) -> ok end),
    meck:new(dalmatiner_opt, [passthrough]),
    meck:expect(dalmatiner_opt, get, fun( _, _, _, _, Dflt) -> Dflt end),
    meck:new(dalmatiner_vacuum, [passthrough]),
    meck:expect(dalmatiner_vacuum, register, fun() ->ok end),
    meck:new(folsom_metrics),
    meck:expect(folsom_metrics, notify, fun(_) -> ok end),
    meck:expect(folsom_metrics, histogram_timed_update,
                fun(_, Mod, Fn, Args) -> apply(Mod, Fn, Args) end),
    ok.

cleanup() ->
    meck:unload(riak_core_metadata),
    meck:unload(dalmatiner_opt),
    meck:unload(dalmatiner_vacuum),
    ok.

-module(qry_parser_eqc).

-ifdef(TEST).
-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-compile(export_all).

-define(P, metric_qry_parser).

non_empty_string() ->
    ?SUCHTHAT(L, list(choose($a, $z)), L =/= "").

diff_strings() ->
    ?SUCHTHAT({S1, S2}, {non_empty_string(), non_empty_string()}, S1 =/= S2).

non_empty_binary() ->
    ?LET(L, non_empty_string(), list_to_binary(L)).


qry_tree() ->
    ?SIZED(Size, oneof([
                        qry_tree(Size),
                        {to_list, qry_tree(Size)}
                       ])).

qry_tree(Size) ->
    ?LAZY(oneof([
                 oneof([
                        {get, non_empty_binary(), {range, int(), int()}},
                        {mget, sum, non_empty_binary(), {range, int(), int()}},
                        {mget, avg, non_empty_binary(), {range, int(), int()}}
                       ]) || Size == 0] ++
              [{derivate, qry_tree(Size -1)} || Size > 0] ++
              [{scale, real(), qry_tree(Size -1)} || Size > 0] ++
              [{min, int(), qry_tree(Size -1)} || Size > 0] ++
              [{max, int(), qry_tree(Size -1)} || Size > 0] ++
              [{sum, int(), qry_tree(Size -1)} || Size > 0] ++
              [{avg, int(), qry_tree(Size -1)} || Size > 0])).


glob() ->
    ?LET({S, G, M}, ?SIZED(Size, glob(Size)),
         {list_to_binary(string:join(S, ".")),
          list_to_binary(string:join(G, ".")),
          M}).

glob(Size) ->
    ?LAZY(oneof([?LET(S, non_empty_string(), {[S], [S], true}) || Size == 0] ++
              [add(Size) || Size > 0])).

add(Size) ->
    frequency(
      [{1, diff_element(Size)},
       {10, matching_element(Size)},
       {10, glob_element(Size)}]).

matching_element(Size) ->
    ?LAZY(
       ?LET(
          {L, G, M}, glob(Size-1),
          ?LET(S, non_empty_string(),
               {[S | L], [S | G], M}))).

diff_element(Size) ->
    ?LAZY(
       ?LET(
          {L, G, _M}, glob(Size-1),
          ?LET({S1, S2}, diff_strings(),
               {[S1 | L], [S2 | G], false}))).

glob_element(Size) ->
    ?LAZY(
       ?LET(
          {L, G, M}, glob(Size-1),
          {[non_empty_string() | L], ["*" | G], M})).

prop_qery_parse_unparse() ->
    ?FORALL(T, qry_tree(),
            T == ?P:parse(?P:unparse(T))).

prop_glob_match() ->
    ?FORALL({S, G, M}, glob(),
            begin
                M ==  ([S] == ?P:glob_match(G, [S]))
            end).

-include("eqc_helper.hrl").
-endif.
-endif.

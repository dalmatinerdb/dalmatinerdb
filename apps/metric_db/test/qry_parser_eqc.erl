-module(qry_parser_eqc).

-ifdef(TEST).
-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-compile(export_all).

-define(P, metric_qry_parser).

non_empty_binary() ->
    ?SUCHTHAT(B, ?LET(L, list(choose($a, $z)), list_to_binary(L)), B =/= <<>>).


qry_tree() ->
    ?SIZED(Size, oneof([
                        qry_tree(Size),
                        {to_list, qry_tree(Size)}
                       ])).

qry_tree(Size) ->
    ?LAZY(oneof([{get, non_empty_binary(), {range, int(), int()}} || Size == 0] ++
              [{derivate, qry_tree(Size -1)} || Size > 0] ++
              [{scale, real(), qry_tree(Size -1)} || Size > 0] ++
              [{min, int(), qry_tree(Size -1)} || Size > 0] ++
              [{max, int(), qry_tree(Size -1)} || Size > 0] ++
              [{sum, int(), qry_tree(Size -1)} || Size > 0] ++
              [{avg, int(), qry_tree(Size -1)} || Size > 0])).

prop_qery_parse_unparse() ->
    ?FORALL(T, qry_tree(),
            T == ?P:parse(?P:unparse(T))).

-include("eqc_helper.hrl").
-endif.
-endif.

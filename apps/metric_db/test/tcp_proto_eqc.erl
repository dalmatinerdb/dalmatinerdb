-module(tcp_proto_eqc).

-ifdef(TEST).
-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-compile(export_all).

non_empty_binary() ->
    ?SUCHTHAT(B, binary(), B =/= <<>>).

non_empty_binary_list() ->
    ?SUCHTHAT(L, list(non_empty_binary()), L =/= []).

prop_encode_decode_metrics() ->
    ?FORALL(L, non_empty_binary_list(),
            begin
                L1 = lists:sort(L),
                B = metric_tcp_proto:encode_metrics(L),
                Rev = metric_tcp_proto:decode_metrics(B),
                L2 = lists:sort(Rev),
                L1 == L2
            end).

prop_encode_decode_get() ->
    ?FORALL({M, T, C}, {non_empty_binary(), choose(0, 5000), choose(1, 5000)},
            {M, T, C} == metric_tcp_proto:decode_get(metric_tcp_proto:encode_get(M, T, C))).

-include("eqc_helper.hrl").
-endif.
-endif.

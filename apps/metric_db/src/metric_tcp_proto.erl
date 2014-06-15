-module(metric_tcp_proto).


-define(METRIC_SIZE, 16).

-export([encode_metrics/1, decode_metrics/1, encode_get/3, decode_get/1]).
-ignore_xref([encode_metrics/1, decode_metrics/1, encode_get/3, decode_get/1]).

encode_metrics(Ms) ->
    Data = << <<(byte_size(M)):?METRIC_SIZE/integer, M/binary>> ||  M <- Ms >>,
    <<(byte_size(Data)):32/integer, Data/binary>>.

decode_metrics(<<S:32/integer, Ms:S/binary>>) ->
    decode_metrics(Ms, []).
decode_metrics(<<>>, Acc) ->
    Acc;

decode_metrics(<<S:?METRIC_SIZE/integer, M:S/binary, R/binary>>, Acc) ->
    decode_metrics(R, [M | Acc]).

decode_get(<<_L:?METRIC_SIZE/integer, M:_L/binary, T:64/integer, C:32/integer>>) ->
    {M, T, C}.

encode_get(M, T, C) ->
    <<(byte_size(M)):?METRIC_SIZE/integer, M/binary, T:64/integer, C:32/integer>>.

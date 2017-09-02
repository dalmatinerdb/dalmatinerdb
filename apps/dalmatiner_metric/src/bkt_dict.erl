-module(bkt_dict).

-include_lib("mmath/include/mmath.hrl").

-export([size/1, new/3, update_chash/1, flush/1, add/4, to_list/1]).

-export_type([bkt_dict/0]).

-record(bkt_dict, {bucket, ppf, dict, n, w, nodes, cbin, size = 0}).

-type bkt_dict() :: #bkt_dict{}.

-spec new(binary(), pos_integer(), pos_integer()) ->
                 bkt_dict().
new(Bkt, N, W) ->
    PPF = dalmatiner_opt:ppf(Bkt),
    Dict = dict:new(),
    update_chash(#bkt_dict{bucket = Bkt, ppf = PPF, dict = Dict, n = N, w = W}).

-spec add(binary(), pos_integer(), binary(), bkt_dict()) ->
                 bkt_dict().
add(Metric, Time, Points, BD = #bkt_dict{ppf = PPF}) ->
    Count = mmath_bin:length(Points),
    ddb_counter:inc(<<"mps">>, Count),
    Splits = mstore:make_splits(Time, Count, PPF),
    insert_metric(Metric, Splits, Points, BD).

-spec flush(bkt_dict()) ->
                 bkt_dict().
flush(BD = #bkt_dict{dict = Dict, w = W, n = N}) ->
    BD1 = #bkt_dict{nodes = Nodes} = update_chash(BD),
    metric:mput(Nodes, Dict, W, N),
    BD1#bkt_dict{dict = dict:new()}.


-spec update_chash(bkt_dict()) ->
                 bkt_dict().
update_chash(BD = #bkt_dict{n = N}) ->
    {ok, CBin} = riak_core_ring_manager:get_chash_bin(),
    Nodes1 = chash:nodes(chashbin:to_chash(CBin)),
    Nodes2 = [{I, riak_core_apl:get_apl(I, N, metric)} || {I, _} <- Nodes1],
    BD#bkt_dict{nodes = Nodes2, cbin = CBin}.

insert_metric(_Metric, [], <<>>, BD) ->
    BD;

insert_metric(Metric, [{Time, Count} | Splits], PointsIn,
               BD = #bkt_dict{bucket = Bucket, cbin = CBin, ppf = PPF,
                              dict = Dict, size = MaxCnt}) ->
    Size = (Count * ?DATA_SIZE),
    <<Points:Size/binary, Rest/binary>> = PointsIn,
    DocIdx = riak_core_util:chash_key({Bucket, {Metric, Time div PPF}}),
    {Idx, _} = chashbin:itr_value(chashbin:exact_iterator(DocIdx, CBin)),
    Dict1 = dict:append(Idx, {Bucket, Metric, Time, Points}, Dict),

    CntName = <<Idx:160/integer, "-count">>,
    BktCnt = dict:fetch(CntName, Dict1) + 1,
    Dict2 = dict:store(CntName, BktCnt, Dict1),

    BD1 = BD#bkt_dict{dict = Dict2},
    BD2 = case BktCnt of
              BktCnt when BktCnt > MaxCnt ->
                  BD1#bkt_dict{size = BktCnt};
              _ ->
                  BD1
          end,
    insert_metric(Metric, Splits, Rest, BD2).

size(#bkt_dict{size = Size}) ->
    Size.

to_list(#bkt_dict{dict = Dict}) ->
    L = dict:fold(fun (_Idx, Es, Acc) when is_integer(_Idx) ->
                          [Es | Acc];
                      (_Idx, _Es, Acc) ->
                          Acc
                  end, [], Dict),
    lists:flatten(L).


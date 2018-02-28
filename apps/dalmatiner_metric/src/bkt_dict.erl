-module(bkt_dict).

-include_lib("mmath/include/mmath.hrl").

-export([size/1, new/3, update_chash/1, flush/1, add/4, to_list/1]).

-export_type([bkt_dict/0]).

-record(bkt_dict, {
          bucket    :: binary(),
          ppf       :: pos_integer(),
          dict      :: ets:tid(),
          counters  :: ets:tid(),
          n         :: pos_integer(),
          w         :: pos_integer(),
          nodes     :: list() | undefined,
          cbin      :: list() | undefined,
          ring_size :: non_neg_integer() | undefined,
          size = 0  :: non_neg_integer()
         }).

-type bkt_dict() :: #bkt_dict{}.

-spec new(binary(), pos_integer(), pos_integer()) ->
                 bkt_dict().
new(Bkt, N, W) ->
    PPF = dalmatiner_opt:ppf(Bkt),
    Dict = ets:new(bkt_dict, [ordered_set]),
    Counters = ets:new(bkt_dict_counters, [ordered_set]),
    update_chash(#bkt_dict{bucket = Bkt,
                           ppf = PPF,
                           dict = Dict,
                           counters = Counters,
                           n = N, w = W}).

-spec add(binary(), pos_integer(), binary(), bkt_dict()) ->
                 bkt_dict().
add(Metric, Time, Points, BD = #bkt_dict{ppf = PPF}) ->
    Count = mmath_bin:length(Points),
    ddb_counter:inc(<<"mps">>, Count),
    Splits = mstore:make_splits(Time, Count, PPF),
    insert_metric(Metric, Splits, Points, BD).

-spec flush(bkt_dict()) ->
                 bkt_dict().
flush(BD = #bkt_dict{dict = Dict, counters = Counters, w = W, n = N}) ->
    BD1 = #bkt_dict{nodes = Nodes} = update_chash(BD),
    metric:mput(Nodes, Dict, W, N),
    ets:delete_all_objects(Dict),
    ets:delete_all_objects(Counters),
    BD1.


-spec update_chash(bkt_dict()) ->
                 bkt_dict().
update_chash(BD = #bkt_dict{n = N}) ->
    {ok, CBin} = riak_core_ring_manager:get_chash_bin(),
    RingSize = chashbin:num_partitions(CBin),
    Nodes1 = chash:nodes(chashbin:to_chash(CBin)),
    Nodes2 = [{I, riak_core_apl:get_apl(I, N, metric)} || {I, _} <- Nodes1],
    BD#bkt_dict{nodes = Nodes2, cbin = CBin, ring_size = RingSize}.


%% We need to a adjust that since we are using the ord dict to lookup
%% And that lookup will result in giving THE FOLLOWING index.
responsible_index(<<HashKey:160/integer>>, Size) ->
    responsible_index(HashKey, Size);
responsible_index(HashKey, Size) ->
    Inc = chash:ring_increment(Size),
    %% (((HashKey div Inc) + 1) rem Size) * Inc.
    %% We had to remove the - 1 ...
    ((HashKey div Inc) rem Size) * Inc.


insert_metric(_Metric, [], <<>>, BD) ->
    BD;

insert_metric(Metric, [{Time, Count} | Splits], PointsIn,
               BD = #bkt_dict{bucket = Bucket, ppf = PPF,
                              dict = Dict, counters = Counters,
                              size = MaxCnt, ring_size = RingSize}) ->
    Size = (Count * ?DATA_SIZE),
    <<Points:Size/binary, Rest/binary>> = PointsIn,
    DocIdx = riak_core_util:chash_key({Bucket, {Metric, Time div PPF}}),
    Idx = responsible_index(DocIdx, RingSize),
    L1 = case ets:lookup(Dict, Idx) of
                [] ->
                 [{Bucket, Metric, Time, Points}];
             [{Idx, L0}] ->
                 [{Bucket, Metric, Time, Points} | L0]
         end,
    ets:insert(Dict, {Idx, L1}),
    BktCnt = ets:update_counter(Counters, Idx, 1, {Idx, 1}),
    BD1 = case BktCnt of
              BktCnt when BktCnt > MaxCnt ->
                  BD#bkt_dict{size = BktCnt};
              _ ->
                  BD
          end,
    insert_metric(Metric, Splits, Rest, BD1).

size(#bkt_dict{size = Size}) ->
    Size.

to_list(#bkt_dict{dict = Dict}) ->
    L = ets:foldl(fun ({_Idx, Es}, Acc) ->
                          [Es | Acc]
                  end, [], Dict),
    lists:flatten(L).

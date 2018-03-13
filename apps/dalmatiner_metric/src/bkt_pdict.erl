-module(bkt_pdict).

-include_lib("mmath/include/mmath.hrl").

-export([size/1, new/3, new/4, update_chash/1, flush/1, add/4, to_list/1]).

-export_type([bkt_dict/0]).

-record(bkt_dict, {
          bucket       :: binary(),
          ppf          :: pos_integer(),
          n            :: pos_integer(),
          w            :: pos_integer(),
          nodes        :: list() | undefined,
          cbin         :: list() | undefined,
          ring_size    :: non_neg_integer() | undefined,
          size = 0     :: non_neg_integer(),
          data_size    :: pos_integer(),
          hpts         :: boolean()
         }).

-type bkt_dict() :: #bkt_dict{}.

-spec new(binary(), pos_integer(), pos_integer()) ->
                 bkt_dict().
new(Bkt, N, W) ->
    new(Bkt, N, W, false).

new(Bkt, N, W, HPTS) ->
    PPF = dalmatiner_opt:ppf(Bkt),
    update_chash(#bkt_dict{
                    bucket = Bkt,
                    ppf = PPF,
                    n = N,
                    w = W,
                    hpts = HPTS,
                    data_size = data_size(HPTS)}).

-spec add(binary(), pos_integer(), binary(), bkt_dict()) ->
                 bkt_dict().
add(Metric, Time, Points, BD = #bkt_dict{ppf = PPF, data_size = DataSize}) ->
    Count = byte_size(Points) div DataSize,
    ddb_counter:inc(<<"mps">>, Count),
    Splits = mstore:make_splits(Time, Count, PPF),
    insert_metric(Metric, Splits, Points, BD).

-spec flush(bkt_dict()) ->
                 bkt_dict().
flush(BD = #bkt_dict{w = W, n = N}) ->
    BD1 = #bkt_dict{nodes = Nodes} = update_chash(BD),
    metric:mput_list(Nodes, get(), W, N),
    erase(),
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
                             size = MaxCnt,
                             data_size = DataSize,
                             ring_size = RingSize}) ->
    Size = (Count * DataSize),
    <<Points:Size/binary, Rest/binary>> = PointsIn,
    DocIdx = riak_core_util:chash_key({Bucket, {Metric, Time div PPF}}),
    Idx = responsible_index(DocIdx, RingSize),
    append(Idx, {Bucket, Metric, Time, Points}),
    BktCnt = case get({count, Idx}) of
                 undefined ->
                     1;
                 N ->
                     N + 1
             end,
    put({count, Idx}, BktCnt),

    BD1 = case BktCnt of
              BktCnt when BktCnt > MaxCnt ->
                  BD#bkt_dict{size = BktCnt};
              _ ->
                  BD
          end,
    insert_metric(Metric, Splits, Rest, BD1).

size(#bkt_dict{size = Size}) ->
    Size.

to_list(#bkt_dict{}) ->
    [V || {K, V} <- get(), is_integer(K)].


data_size(true) ->
    ?DATA_SIZE * 2;
data_size(false) ->
    ?DATA_SIZE.


append(Key, Value) ->
    L1 = case get(Key) of
             undefined ->
                 [Value];
             L0 ->
                 [Value | L0]
         end,
    put(Key, L1),
    ok.

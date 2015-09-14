-module(dalmatiner_opt).

-ifdef(TEST).
-export([valid_type/2]).
-endif.

-define(WEEK, 604800). %% Seconds in a week.

-export([get/5, set/2, resolution/1, lifetime/1, ppf/1]).
-ignore_xref([get/5, set/2]).

get(Prefix, SubPrefix, Key, {EnvApp, EnvKey}, Dflt) ->
    P = ensure_bin(Prefix),
    SP = ensure_bin(SubPrefix),
    case riak_core_metadata:get({P, SP}, Key) of
        undefined ->
            V = case application:get_env(EnvApp, EnvKey) of
                    {ok, Val} ->
                        Val;
                    undefined ->
                        Dflt
                end,
            set(P, SP, Key, V),
            V;
        V ->
            V
    end.

set(Ks, Val) ->
    case is_valid(Ks, Val) of
        {true, V1} ->
            [Prefix, SubPrefix, Key] =
                [ensure_bin(K) || K <- Ks],
            set(Prefix, SubPrefix, Key, V1);
        E ->
            E
    end.

set(Prefix, SubPrefix, Key, Val) ->
    riak_core_metadata:put({Prefix, SubPrefix}, Key, Val).

is_valid(Ks, V) ->
    Ks1 = [ensure_str(K) || K <- Ks],
    case get_type(Ks1) of
        {ok, Type} ->
            case  valid_type(Type, V) of
                {true, V1} ->
                    {true, V1};
                false ->
                    {invalid, type, Type}
            end;
        E ->
            E
    end.


resolution(Bucket) ->
    dalmatiner_opt:get(
      <<"buckets">>, Bucket, <<"resolution">>,
      {metric_vnode, resolution}, 1000).

lifetime(Bucket) ->
    dalmatiner_opt:get(
      <<"buckets">>, Bucket, <<"lifetime">>,
      {metric_vnode, lifetime}, infinity).

ppf(Bucket) ->
    dalmatiner_opt:get(<<"buckets">>, Bucket,
                       <<"points_per_file">>,
                       {metric_vnode, points_per_file}, ?WEEK).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

opts() ->
    [{"buckets",
      [{'_', [{"data_dir", string},
              {"resolution", integer},
              {"cache_points", integer},
              {"points_per_file", integer},
              {"lifetime", integer},
              {"chash_size", integer}]}]}].

get_type(Ks) ->
    get_type(Ks, opts()).

get_type([], _) ->
    {invalid, path};

get_type([K], Os) ->
    case proplists:get_value(K, Os) of
        undefined ->
            {invalid, key, K};
        Type ->
            {ok, Type}
    end;

get_type([K|R], Os) ->
    case proplists:get_value(K, Os) of
        undefined ->
            case proplists:get_value('_', Os) of
                undefined ->
                    {invalid, key, K};
                Os1 ->
                    get_type(R, Os1)
            end;
        Os1 ->
            get_type(R, Os1)
    end.

valid_type(integer, I) when is_list(I) ->
    try list_to_integer(I) of
        V ->
            {true, V}
    catch
        _:_ ->
            false
    end;
valid_type(integer, I) when is_integer(I) ->
    {true, I};

valid_type(float, I) when is_list(I) ->
    try list_to_float(I) of
        V ->
            {true, V}
    catch
        _:_ ->
            false
    end;
valid_type(float, I) when is_float(I) ->
    {true, I};

valid_type(string, L) when is_binary(L) ->
    {true, binary_to_list(L)};
valid_type(string, L) when is_list(L) ->
    {true, L};

valid_type(binary, B) when is_binary(B) ->
    {true, B};
valid_type(binary, L) when is_list(L) ->
    {true, list_to_binary(L)};

valid_type({enum, Vs}, V) when is_atom(V)->
    valid_type({enum, Vs}, atom_to_list(V));
valid_type({enum, Vs}, V) when is_binary(V)->
    valid_type({enum, Vs}, binary_to_list(V));
valid_type({enum, Vs}, V) ->
    case lists:member(V, Vs) of
        true ->
            {true, list_to_atom(V)};
        false ->
            false
    end;

valid_type(_, _) ->
    false.

ensure_str(V) when is_atom(V) ->
    atom_to_list(V);
ensure_str(V) when is_binary(V) ->
    binary_to_list(V);
ensure_str(V) when is_integer(V) ->
    integer_to_list(V);
ensure_str(V) when is_float(V) ->
    float_to_list(V);
ensure_str(V) when is_list(V) ->
    V.

ensure_bin(B) when is_binary(B) ->
    B;
ensure_bin(O) ->
    list_to_binary(ensure_str(O)).


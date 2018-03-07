-module(dalmatiner_opt).

-define(WEEK, 604800). %% Seconds in a week.

-export([resolution/1, set_resolution/2,
         lifetime/1, set_lifetime/2,
         ppf/1, set_ppf/2,
         grace/1, set_grace/2,
         delete/1,
         set_hpts/2,
         hpts/1,
         bucket_exists/1]).
-ignore_xref([set_resolution/2]).

-define(MAIN, <<"buckets">>).
-define(HPTS, {?MAIN, <<"hpts">>}).
-define(RES, {?MAIN, <<"resolution">>}).
-define(PPF, {?MAIN, <<"points_per_file">>}).

resolution(Bucket) when is_binary(Bucket) ->
    {ok, R} = get(?MAIN, <<"resolution">>, Bucket),
    R.

set_resolution(Bucket, Resolution)
  when is_binary(Bucket),
       is_integer(Resolution),
       Resolution > 0 ->
    set(?MAIN, <<"resolution">>, Bucket, Resolution).

delete_resolution(Bucket) when is_binary(Bucket) ->
    riak_core_metadata:delete({?MAIN, <<"resolution">>}, Bucket).

hpts(Bucket) when is_binary(Bucket) ->
    {ok, R} = get(?MAIN, <<"hpts">>, Bucket),
    R.

set_hpts(Bucket, HPTS)
  when is_binary(Bucket),
       (HPTS == true orelse HPTS == false) ->
    set(?MAIN, <<"hpts">>, Bucket, HPTS).

delete_hpts(Bucket) when is_binary(Bucket) ->
    riak_core_metadata:delete({?MAIN, <<"hpts">>}, Bucket).

lifetime(Bucket) when is_binary(Bucket) ->
    get_or_set(?MAIN, <<"lifetime">>, Bucket,
               {metric_vnode, lifetime}, infinity).

set_lifetime(Bucket, TTL) when is_binary(Bucket), is_integer(TTL), TTL > 0 ->
    set(?MAIN, <<"lifetime">>, Bucket, TTL);
set_lifetime(Bucket, infinity) when is_binary(Bucket) ->
    set(?MAIN, <<"lifetime">>, Bucket, infinity).

delete_lifetime(Bucket) when is_binary(Bucket) ->
    riak_core_metadata:delete({?MAIN, <<"lifetime">>}, Bucket).

ppf(Bucket) when is_binary(Bucket) ->
    {ok, R} = get(?MAIN, <<"points_per_file">>, Bucket),
    R.

set_ppf(Bucket, PPF) ->
    set(?MAIN, <<"points_per_file">>, Bucket, PPF).


delete_ppf(Bucket) when is_binary(Bucket) ->
    riak_core_metadata:delete({?MAIN, <<"points_per_file">>}, Bucket).

set_grace(Bucket, GRACE) ->
    set(?MAIN, <<"grace">>, Bucket, GRACE).

grace(Bucket) when is_binary(Bucket) ->
    grace(Bucket, 0).

grace(Bucket, Dflt) when is_binary(Bucket) ->
    get_or_set(?MAIN, <<"grace">>, Bucket,
               {metric_vnode, grace}, Dflt).

delete_grace(Bucket) when is_binary(Bucket) ->
    riak_core_metadata:delete({?MAIN, <<"grace">>}, Bucket).

delete(Bucket) ->
    delete_ppf(Bucket),
    delete_hpts(Bucket),
    delete_lifetime(Bucket),
    delete_grace(Bucket),
    delete_resolution(Bucket).

bucket_exists(Bucket) ->
    riak_core_metadata:get(?HPTS, Bucket) =/= undefined andalso
        riak_core_metadata:get(?RES, Bucket) =/= undefined andalso
        riak_core_metadata:get(?PPF, Bucket) =/= undefined.

%%%===================================================================
%%% Internal Functions
%%%===================================================================



get_or_set(Prefix, SubPrefix, Key, {EnvApp, EnvKey}, Dflt) ->
    case riak_core_metadata:get({Prefix, SubPrefix}, Key) of
        undefined ->
            V = get_dflt(Prefix, SubPrefix, Key, {EnvApp, EnvKey}, Dflt),
            set(Prefix, SubPrefix, Key, V),
            V;
        V ->
            V
    end.

get(Prefix, SubPrefix, Key) ->
    case riak_core_metadata:get({Prefix, SubPrefix}, Key) of
        undefined ->
            undefined;
        V ->
            {ok, V}
    end.

get_dflt(Prefix, SubPrefix, Key, {EnvApp, EnvKey}, Dflt) ->
    %% This is hacky but some data was stored in reverse order
    %% before.
    case riak_core_metadata:get({Prefix, Key}, SubPrefix) of
        undefined ->
            case application:get_env(EnvApp, EnvKey) of
                {ok, Val} ->
                    Val;
                undefined ->
                    Dflt
            end;
        V ->
            riak_core_metadata:delete({Prefix, Key}, SubPrefix),
            V
    end.


set(Prefix, SubPrefix, Key, Val) ->
    riak_core_metadata:put({Prefix, SubPrefix}, Key, Val).

-module(dalmatiner_opt).

-export([resolution/1, lifetime/1, ppf/1, set_resolution/2,
         set_lifetime/2, delete/1]).

-ignore_xref([set_resolution/2]).

-define(WEEK, 604800). %% Seconds in a week.
-define(ENV_KEY, metric_vnode).
-define(PREFIX, <<"buckets">>).
-define(RESOLUTION, <<"resolution">>).
-define(LIFETIME, <<"lifetime">>).
-define(PPF, <<"points_per_file">>).

resolution(Bucket) when is_binary(Bucket) ->
    get(?PREFIX, ?RESOLUTION, Bucket, {?ENV_KEY, resolution}, 1000).

set_resolution(Bucket, Resolution)
  when is_binary(Bucket),
       is_integer(Resolution),
       Resolution > 0 ->

    %% Resolution cannot be changed after bucket creation, existing bucket
    %% resolution has to match what is supplied. Therefore a coverage query is
    %% not required once a value is set, as in the case of TTL.
    case riak_core_metadata:get({?PREFIX, ?RESOLUTION}, Bucket) of
        undefined ->
            set(?PREFIX, ?RESOLUTION, Bucket, Resolution);
        Resolution ->
            ok
    end.

lifetime(Bucket) when is_binary(Bucket) ->
    get(?PREFIX, ?LIFETIME, Bucket, {?ENV_KEY, lifetime},
        infinity).

set_lifetime(Bucket, TTL) when is_binary(Bucket), is_integer(TTL), TTL > 0 ->
    set(?PREFIX, ?LIFETIME, Bucket, TTL);
set_lifetime(Bucket, infinity) when is_binary(Bucket) ->
    set(?PREFIX, ?LIFETIME, Bucket, infinity).

ppf(Bucket) when is_binary(Bucket) ->
    get(?PREFIX, ?PPF, Bucket, {?ENV_KEY, points_per_file}, ?WEEK).

delete(Bucket) ->
    delete_ppf(Bucket),
    delete_lifetime(Bucket),
    delete_resolution(Bucket).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

get(Prefix, SubPrefix, Key, {EnvApp, EnvKey}, Dflt) ->
    case riak_core_metadata:get({Prefix, SubPrefix}, Key) of
        undefined ->
            V = get_dflt(Prefix, SubPrefix, Key, {EnvApp, EnvKey}, Dflt),
            set(Prefix, SubPrefix, Key, V),
            V;
        V ->
            V
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

delete_resolution(Bucket) when is_binary(Bucket) ->
    riak_core_metadata:delete({?PREFIX, ?RESOLUTION}, Bucket).

delete_lifetime(Bucket) when is_binary(Bucket) ->
    riak_core_metadata:delete({?PREFIX, ?LIFETIME}, Bucket).

delete_ppf(Bucket) when is_binary(Bucket) ->
    riak_core_metadata:delete({?PREFIX, ?PPF}, Bucket).

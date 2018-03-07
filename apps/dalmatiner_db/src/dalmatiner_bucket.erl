%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@project-fifo.net>
%%% @copyright (C) 2016, Project-FiFo UG
%%% @doc
%%%
%%% @end
%%% Created : 12 Nov 2016 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(dalmatiner_bucket).

-export([list/0, info/1]).

-spec list() -> [binary()].
list() ->
    Bs = riak_core_metadata:to_list({<<"buckets">>, <<"resolution">>}),
    [B || {B, R} <- Bs, R /= ['$deleted']].

-spec info(binary()) ->
                  {ok, dproto_tcp:bucket_info()} |
                  {error, missing}.

info(Bucket) ->
    case dalmatiner_opt:bucket_exists(Bucket) of
        true ->
            Resolution = dalmatiner_opt:resolution(Bucket),
            PPF = dalmatiner_opt:ppf(Bucket),
            TTL = dalmatiner_opt:lifetime(Bucket),
            Grace = dalmatiner_opt:grace(Bucket),
            HPTS = dalmatiner_opt:hpts(Bucket),
            {ok, #{
               resolution => Resolution,
               ppf => PPF,
               ttl => TTL,
               grace => Grace,
               hpts => HPTS
              }};
        false ->
            {error, missing}
    end.

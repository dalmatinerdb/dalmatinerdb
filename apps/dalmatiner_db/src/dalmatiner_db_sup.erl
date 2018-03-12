-module(dalmatiner_db_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    WriteFSMs = {dalmatiner_write_fsm_sup,
                 {dalmatiner_write_fsm_sup, start_link, []},
                 permanent, infinity, supervisor, [dalmatiner_write_fsm_sup]},
    ReadFSMs = {dalmatiner_read_fsm_sup,
                {dalmatiner_read_fsm_sup, start_link, []},
                permanent, infinity, supervisor, [dalmatiner_read_fsm_sup]},
    fix_non_hpts_buckets(),
    ensure_ddb(),
    {ok, {{one_for_one, 5, 10},
          [WriteFSMs, ReadFSMs]}}.


fix_non_hpts_buckets() ->
    Bs = riak_core_metadata:to_list({<<"buckets">>, <<"resolution">>}),
    [case riak_core_metadata:get({<<"buckets">>, <<"hpts">>}, B) of
         undefined ->
             dalmatiner_opt:set_hpts(B, false);
         _ ->
             ok
     end|| {B, R} <- Bs, R /= ['$deleted']].


ensure_ddb() ->
    Bucket = <<"dalmatinerdb">>,
    Res = cuttlefish_datatypes:from_string(
            "1s", {duration, ms}),

    PPF = cuttlefish_datatypes:from_string(
            "1w", {duration, ms}) div Res,
    case dalmatiner_opt:bucket_exists(Bucket) of
        true ->
            ok;
        false ->
            dalmatiner_opt:set_resolution(Bucket, Res),
            dalmatiner_opt:set_ppf(Bucket, PPF),
            dalmatiner_opt:set_hpts(Bucket, false)
    end.

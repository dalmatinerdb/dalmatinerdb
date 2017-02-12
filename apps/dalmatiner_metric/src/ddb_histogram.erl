-module(ddb_histogram).

-export([register/1, timed_update/2, timed_update/4]).

register(Name) ->
    folsom_metrics:new_histogram(Name, slide, 60).

timed_update(Name, Fun) ->
    folsom_metrics:histogram_timed_update(Name, Fun).

timed_update(Name, Mod, Fun, Args) ->
    folsom_metrics:histogram_timed_update(Name, Mod, Fun, Args).

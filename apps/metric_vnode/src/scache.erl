%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@schroedinger.local>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%% A simple caching algorithm for metric blogs that allows for compaction
%%% @end
%%% Created :  7 Sep 2014 by Heinz Nikolaus Gies <heinz@schroedinger.local>
%%%-------------------------------------------------------------------
-module(scache).

-export([add/3, compact/1, realize/1]).


add(T, V, [{T0, V0} | C]) 
  when T == ((T0 * -1) + byte_size(V0) div 9) ->
	add(T0*-1, <<V0/binary, V/binary>>, C);

add(T, V, C) ->
	ordsets:add_element({T*-1, V}, C).


compact([]) ->
	[];
compact([{T, V}]) ->
	[{T, V}];
compact([{T0, V0}, {T1, V1} | R])
  when (T0*-1) == ((T1*-1) + byte_size(V1) div 9) ->
	compact([{T1, <<V1/binary, V0/binary>>} | R]);
compact([{T, V} | R]) ->
	[{T, V} | compact(R)].


realize(C) ->
	realize(C, []).

realize([], Acc) ->
	Acc;
realize([{T, V}], Acc) ->
	[{T*-1, V} | Acc];
realize([{T0, V0}, {T1, V1} | R], Acc)
  when (T0*-1) == ((T1*-1) + byte_size(V1) div 9) ->
	realize([{T1, <<V1/binary, V0/binary>>} | R], Acc);
realize([{T, V} | R], Acc) ->
	realize(R, [{T*-1, V} | Acc]).


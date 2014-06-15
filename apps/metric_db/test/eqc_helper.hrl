%% Include this file at the END of _eqc.erl file!
%%
%% Allows running EQC in both EQC-CI and EQC offline. Placing eqc files in the
%% test directory will allow EUnit to automatically discover EQC tests by the
%% same rules EQC-CI does.
%% Also when running on the console output is nice and colored. The following
%% Makefile rules are handy when using rebar:
%%
%% qc: clean all
%%        $(REBAR) -C rebar_eqc.config compile eunit skip_deps=true --verbose
%%
%% eqc-ci: clean all
%%        $(REBAR) -D EQC_CI -C rebar_eqc.config compile eunit skip_deps=true --verbose
%%
%% The corresponding .eqc_ci file would look like this:
%%
%%{build, "make eqc-ci"}.
%%{test_path, "."}.
%%
%% EQC_NUM_TESTS and EQC_EUNIT_TIMEUT can be -defined in the file including this
%% if not they'll defualt to 500 tests and 60s


-ifdef(EQC_CI).
-define(OUT(P),  on_output(fun(S,F) -> io:fwrite(user, S, F) end, P)).
-else.
-define(OUT(P),
        on_output(fun
                      (".", []) ->
                          io:fwrite(user, <<"\e[0;32m*\e[0m">>, []);
                     ("x", []) ->
                          io:format(user, <<"\e[0;33mx\e[0m">>, []);
                     ("Failed! ", []) ->
                          io:format(user, <<"\e[0;31mFailed! \e[0m">>, []);
                     (S, F) ->
                          io:format(user, S, F)
                  end, P)).
-endif.

-ifndef(EQC_NUM_TESTS).
-define(EQC_NUM_TESTS, 500).
-endif.

-ifndef(EQC_EUNIT_TIMEUT).
-define(EQC_EUNIT_TIMEUT, 60).
-endif.

run_test_() ->
    [{exports, E} | _] = module_info(),
    E1 = [{atom_to_list(N), N} || {N, 0} <- E],
    E2 = [{N, A} || {"prop_" ++ N, A} <- E1],
    [{"Running " ++ N ++ " propperty test",
      {timeout, ?EQC_EUNIT_TIMEUT, ?_assert(quickcheck(numtests(?EQC_NUM_TESTS,  ?OUT(?MODULE:A()))))}}
     || {N, A} <- E2].

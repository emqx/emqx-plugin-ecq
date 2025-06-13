%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ecq_gc_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(RETENTION, 1000).
-define(SCAN_LIMIT, 9).
-define(SCAN_DELAY, 1).

%%--------------------------------------------------------------------
%% CT boilerplate
%%--------------------------------------------------------------------

all() -> emqx_ecq_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    %% speed up the test by setting a small scan limit and delay
    persistent_term:put(emqx_ecq_gc_scan_limit, ?SCAN_LIMIT),
    persistent_term:put(emqx_ecq_gc_scan_delay, ?SCAN_DELAY),
    meck:new(emqx_ecq_config, [passthrough]),
    meck:expect(emqx_ecq_config, get_data_retention, fun() -> ?RETENTION end),
    meck:expect(emqx_ecq_config, get_gc_interval, fun() -> 1000 end),
    Config.

end_per_testcase(_Case, _Config) ->
    meck:unload(emqx_ecq_config),
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_continue_gc(_Config) ->
    Tester = self(),
    meck:new(emqx_ecq_store, [passthrough]),
    %% always return continue
    meck:expect(emqx_ecq_store, gc, infinite_gc_loop_fn(Tester)),
    try
        emqx_ecq_gc:start_link(),
        %% trigger gc immediately
        emqx_ecq_gc:run(),
        %% expect gc_begin, then endless 'continue' messages,
        %% we only check 10
        ExpectedMsgs = [<<>> | [integer_to_binary(N) || N <- lists:seq(1, 10)]],
        lists:foreach(
            fun(Msg) ->
                receive
                    {last, Msg} ->
                        ok;
                    Other ->
                        error({unexpected_message, Other})
                end
            end,
            ExpectedMsgs
        )
    after
        ok = gen_server:stop(emqx_ecq_gc),
        meck:unload(emqx_ecq_store)
    end.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

infinite_gc_loop_fn(Tester) ->
    gc_loop_fn(Tester, 100000000).

gc_loop_fn(Tester, Max) ->
    fun(ClientID, ExpireAt) ->
        Now = erlang:system_time(millisecond),
        ?assert(ExpireAt =< Now - ?RETENTION),
        Tester ! {last, ClientID},
        N =
            case get(last) of
                undefined ->
                    1;
                X ->
                    X + 1
            end,
        put(last, N),
        case N > Max of
            true ->
                complete;
            false ->
                {continue, integer_to_binary(N)}
        end
    end.

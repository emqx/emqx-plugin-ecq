%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ecq_config_tests).

-include_lib("eunit/include/eunit.hrl").

parse_test_() ->
    [
        ?_assertEqual(#{data_retention => 10000}, parse(#{<<"data_retention">> => <<"10s">>})),
        ?_assertEqual(#{data_retention => 10000}, parse(#{<<"data_retention">> => <<"10000">>})),
        ?_assertEqual(#{data_retention => 10000}, parse(#{<<"data_retention">> => <<"10000ms">>})),
        ?_assertEqual(#{data_retention => 10000}, parse(#{<<"data_retention">> => 9999.9})),
        ?_assertEqual(#{data_retention => 10000}, parse(#{<<"data_retention">> => 9999.1})),
        ?_assertEqual(#{gc_interval => 3600000}, parse(#{<<"gc_interval">> => <<"1h">>})),
        ?_assertEqual(#{gc_interval => 3600000}, parse(#{<<"gc_interval">> => 3600000})),
        ?_assertEqual(#{reader_batch_size => 16}, parse(#{<<"reader_batch_size">> => 16})),
        ?_assertThrow(
            "reader_batch_size_must_be_less_than_32", parse(#{<<"reader_batch_size">> => 33})
        ),
        ?_assertThrow("reader_batch_size_must_be_positive", parse(#{<<"reader_batch_size">> => 0})),
        ?_assertEqual(#{writer_pool_size => 16}, parse(#{<<"writer_pool_size">> => 16})),
        ?_assertThrow("invalid_writer_pool_size", parse(#{<<"writer_pool_size">> => -1})),
        ?_assertThrow("invalid_writer_pool_size", parse(#{<<"writer_pool_size">> => 10241}))
    ].

parse(Config) ->
    emqx_ecq_config:parse(Config).

%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ecq_config).

-compile({no_auto_import, [get/0, put/1]}).
-export([get/0, put/1, parse/1]).

%% Config getters
-export([
    get_data_retention/0,
    my_role/0,
    get_reader_batch_size/0,
    get_write_timeout/0,
    get_read_timeout/0
]).

-include("emqx_ecq.hrl").

my_role() ->
    persistent_term:get(?ROLE_PT_KEY).

-spec get() -> map().
get() ->
    persistent_term:get(?MODULE).

-spec put(map()) -> ok.
put(Parsed) ->
    persistent_term:put(?MODULE, Parsed).

%% @doc Parse the config.
%% Avro schema does certain validation for the config,
%% but we still need to convert the config to the internal format
%% and also some extra validation like value range check.
-spec parse(map()) -> map().
parse(Config) ->
    maps:fold(
        fun(Key, Value, Acc) ->
            {Key1, Parsed} = parse(Key, Value),
            maps:put(Key1, Parsed, Acc)
        end,
        #{},
        Config
    ).

%% @doc Get the data retention period.
%% Duration in milliseconds.
-spec get_data_retention() -> integer().
get_data_retention() ->
    maps:get(data_retention, get()).

%% @doc Get the reader batch size.
-spec get_reader_batch_size() -> integer().
get_reader_batch_size() ->
    maps:get(reader_batch_size, get()).

%% @doc Get the write timeout.
-spec get_write_timeout() -> integer().
get_write_timeout() ->
    maps:get(write_timeout, get()).

%% @doc Get the read timeout.
-spec get_read_timeout() -> integer().
get_read_timeout() ->
    maps:get(read_timeout, get()).

parse(<<"data_retention">>, Str) ->
    {data_retention, to_duration_ms(Str)};
parse(<<"writer_pool_size">>, Size) ->
    (Size < 0 orelse Size > 10240) andalso throw("invalid_writer_pool_size"),
    {writer_pool_size, Size};
parse(<<"reader_batch_size">>, Size) ->
    %% 32 is the default Receive-Maximum for EMQX
    Size =< 0 andalso throw("reader_batch_size_must_be_positive"),
    Size >= 32 andalso throw("reader_batch_size_must_be_less_than_32"),
    {reader_batch_size, Size};
parse(<<"write_timeout">>, Str) ->
    {write_timeout, to_duration_ms(Str)};
parse(<<"read_timeout">>, Str) ->
    {read_timeout, to_duration_ms(Str)}.

to_duration_ms(Str) ->
    case hocon_postprocess:duration(Str) of
        D when is_number(D) ->
            erlang:ceil(D);
        _ ->
            case to_integer(Str) of
                I when is_integer(I) -> I;
                _ -> throw("invalid_duration_for_data_retention")
            end
    end.

to_integer(Str) when is_binary(Str) ->
    case string:to_integer(Str) of
        {Int, <<>>} -> Int;
        _ -> error
    end.

%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ecq_store).

%% Bootstrapping
-export([
    db_settings/1,
    open_db/0
]).

%% Runtime
-export([
    append/4,
    init_read_state/1,
    ack_and_fetch_next_batch/2
]).

-export_type([msg/0]).

-include("emqx_ecq.hrl").

-define(SCHEMA_VERSION, 1).

%% Topics

-define(PAYLOAD_TOPIC_SEGMENT, <<"payload">>).
-define(KEY_TOPIC_SEGMENT, <<"msg_key">>).
-define(READ_STATE_TOPIC_SEGMENT, <<"read_state">>).
-define(MSG_KEY_TOPIC(CLIENT_ID, MSG_KEY), [
    ?PAYLOAD_TOPIC_SEGMENT, CLIENT_ID, ?KEY_TOPIC_SEGMENT, MSG_KEY
]).
-define(READ_STATE_TOPIC(CLIENT_ID), [?READ_STATE_TOPIC_SEGMENT, CLIENT_ID]).

%% Serialization keys

-define(read_state_schema_version, 1).
-define(read_state_it, 2).

-define(msg_schema_version, 1).
-define(msg_ts, 2).
-define(msg_payload, 3).

-define(PAYLOAD_TX_RETRIES, 1).
-define(READ_STATE_INIT_TX_RETRIES, 5).
-define(READ_STATE_UPDATE_TX_RETRIES, 1).

-define(DB_PAYLOAD_LTS_SETTINGS, #{
    %% "payload/CLIENT_ID/msg_key/MSG_KEY"
    lts_threshold_spec => {simple, {100, 0, 100, 0, 100}}
}).

-define(DB_READ_STATE_LTS_SETTINGS, #{
    %% "read_state/CLIENT_ID"
    lts_threshold_spec => {simple, {100, 0, 10}}
}).

%% Types

-type msg() :: #{seqno := seqno(), msg_key := msg_key(), payload := binary()}.

-type read_state() :: #{it := non_neg_integer(), clientid := binary()}.

open_db() ->
    maybe
        ok ?= open_db(?DB_PAYLOAD, db_settings(?DB_PAYLOAD_LTS_SETTINGS)),
        ok = open_db(?DB_READ_STATE, db_settings(?DB_READ_STATE_LTS_SETTINGS))
    end.

open_db(DB, Settings) ->
    Result = emqx_ds:open_db(DB, Settings),
    ?LOG(info, open_ds_db, #{
        db => DB,
        settings => Settings,
        result => Result
    }),
    Result.

db_settings(LtsSettings) ->
    #{
        transaction =>
            #{
                flush_interval => 5000,
                idle_flush_interval => 1,
                conflict_window => 5000
            },
        storage =>
            {emqx_ds_storage_skipstream_lts_v2, LtsSettings},
        store_ttv => true,
        backend => builtin_raft,
        n_shards => 16,
        replication_options => #{},
        n_sites => 3,
        replication_factor => 2
    }.

-spec append(ClientID :: binary(), _MsgKeyMsgKey :: binary(), _Payload :: binary(), _Ts :: ts()) ->
    ok | {error, term()}.
append(ClientID, MsgKey, Payload, Ts) ->
    TxOpts = #{
        db => ?DB_PAYLOAD,
        shard => {auto, ClientID},
        %% TODO: use several generations for retention
        generation => 1,
        sync => true,
        retries => ?PAYLOAD_TX_RETRIES
    },
    PayloadTopic = ?MSG_KEY_TOPIC(ClientID, MsgKey),
    PayloadBin = pack_msg(Payload, Ts),
    TxFun = fun() ->
        emqx_ds:tx_del_topic(PayloadTopic),
        emqx_ds:tx_write(PayloadTopic, ?ds_tx_ts_monotonic, PayloadBin)
    end,
    case emqx_ds:trans(TxOpts, TxFun) of
        {atomic, _Serial, ok} ->
            ok;
        {error, IsRecoverable, Reason} ->
            {error, {IsRecoverable, Reason}}
    end.

%% @doc Ack and fetch a batch of messages starting from the given seqno.
-spec ack_and_fetch_next_batch(read_state(), _Limit :: non_neg_integer()) ->
    {ok, [msg()], read_state()}.
ack_and_fetch_next_batch(ReadState, Limit) ->
    maybe
        ok ?= persist_read_state(ReadState),
        {ok, Batch} ?= fetch_batch(ReadState, Limit),
        {ok, Batch, ReadState}
    end.

fetch_batch(ReadState, Limit) ->
    case get_iterator(ReadState) of
        {error, _} = Error ->
            Error;
        {ok, undefined} ->
            {ok, [], ReadState};
        {ok, It0} ->
            case emqx_ds:next(It0, Limit) of
                {ok, It, Values} ->
                    Msgs = lists:map(fun unpack_msg/1, Values),
                    {ok, Msgs, ReadState#{it => It}};
                {ok, end_of_stream} ->
                    %% TODO
                    %% Handle generation rotation
                    {ok, [], ReadState};
                {error, IsRecoverable, Reason} ->
                    {error, {IsRecoverable, Reason}}
            end
    end.

get_iterator(#{clientid := ClientID, it := undefined}) ->
    Filter = ?MSG_KEY_TOPIC(ClientID, '#'),
    Shard = emqx_gs:shard_of(ClientID),
    maybe
        {[{_Slab, Stream}], []} ?= emqx_gs:get_streams(?DB_PAYLOAD, Filter, 0, #{shard => Shard}),
        {ok, It} ?= emqx_gs:make_iterator(?DB_PAYLOAD, Stream, Filter, 0),
        {ok, It}
    else
        {[], []} ->
            {ok, undefined};
        {MultipleStreams, []} when is_list(MultipleStreams) ->
            ?LOG(error, "multiple_streams", #{multiple_streams => MultipleStreams, shard => Shard}),
            {error, {multiple_streams, MultipleStreams}};
        {_, Errors} when is_list(Errors) ->
            ?LOG(error, "shard_errors", #{errors => Errors, shard => Shard}),
            {error, {shard_errors, Errors}};
        {error, IsRecoverable, Reason} ->
            {error, {IsRecoverable, Reason}}
    end;
get_iterator(#{it := It}) ->
    {ok, It}.

init_read_state(ClientID) ->
    TxOpts = #{
        db => ?DB_READ_STATE,
        shard => {auto, ClientID},
        sync => true,
        retries => ?READ_STATE_INIT_TX_RETRIES
    },
    TxFun = fun() ->
        emqx_ds:tx_read(?READ_STATE_TOPIC(ClientID))
    end,
    case emqx_ds:trans(TxOpts, TxFun) of
        [{atomic, _Serial, Values}] ->
            case Values of
                [{_Topic, 0, Bin}] ->
                    {ok, unpack_read_state(ClientID, Bin)};
                [] ->
                    {ok, new_read_state(ClientID)}
            end;
        {error, IsRecoverable, Reason} ->
            {error, {IsRecoverable, Reason}}
    end.

persist_read_state(#{clientid := ClientID} = ReadState) ->
    TxOpts = #{
        db => ?DB_READ_STATE,
        shard => {auto, ClientID},
        sync => true,
        retries => ?READ_STATE_UPDATE_TX_RETRIES
    },
    Bin = pack_read_state(ReadState),
    TxFun = fun() ->
        emqx_ds:tx_write({?READ_STATE_TOPIC(ClientID), 0, Bin})
    end,
    case emqx_ds:trans(TxOpts, TxFun) of
        {atomic, _Serial, Res} ->
            Res;
        {error, IsRecoverable, Reason} ->
            {error, {IsRecoverable, Reason}}
    end.

new_read_state(ClientID) ->
    #{clientid => ClientID, it => undefined}.

%%--------------------------------------------------------------------
%% Serialization
%%--------------------------------------------------------------------

pack_msg(Payload, Ts) ->
    term_to_binary(#{
        ?msg_schema_version => ?SCHEMA_VERSION,
        ?msg_ts => Ts,
        ?msg_payload => Payload
    }).

unpack_msg({?MSG_KEY_TOPIC(_ClientID, MsgKey), Seqno, Payload}) ->
    %% NOTE
    %% By far we do not need Ts
    #{
        ?msg_schema_version := ?SCHEMA_VERSION,
        ?msg_payload := Value
    } = binary_to_term(Payload),
    #{
        seqno => Seqno,
        msg_key => MsgKey,
        payload => Value
    }.

pack_read_state(#{it := It}) ->
    term_to_binary(#{
        ?read_state_schema_version => ?SCHEMA_VERSION,
        ?read_state_it => It
    }).

unpack_read_state(ClientId, Bin) ->
    #{
        ?read_state_schema_version := ?SCHEMA_VERSION,
        ?read_state_it := It
    } = binary_to_term(Bin),
    #{it => It, clientid => ClientId}.

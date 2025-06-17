%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ecq_store).

%% Bootstrapping
-export([
    ds_db_settings/0,
    open_ds_db/0
]).

%% Runtime
-export([
    append/4,
    ack_and_fetch_next_batch/3,
    mark_acked/2
]).

-export_type([msg/0]).

-include("emqx_ecq.hrl").

-type msg() :: #{seqno := seqno(), msg_key := msg_key(), payload := binary()}.

-define(DS_DB_SETTINGS, #{
    transaction =>
        #{
            flush_interval => 5000,
            idle_flush_interval => 1,
            conflict_window => 5000
        },
    storage =>
        {emqx_ds_storage_skipstream_lts_v2, #{}},
    store_ttv => true,
    backend => builtin_raft,
    n_shards => 16,
    replication_options => #{},
    n_sites => 3,
    replication_factor => 2
}).

open_ds_db() ->
    Result = emqx_ds:open_db(?DS_DB, ds_db_settings()),
    ?LOG(info, open_ds_db, #{
        result => Result
    }),
    Result.

ds_db_settings() ->
    ?DS_DB_SETTINGS.

-spec append(ClientID :: binary(), MsgKey :: binary(), Payload :: binary(), Ts :: ts()) ->
    ok | {error, term()}.
append(ClientID, MsgKey, Payload, Ts) ->
    TxOpts = #{
        db => ?DS_DB,
        shard => {auto, ClientID},
        %% TODO: use several generations for retention
        generation => 1,
        sync => true,
        retries => 1
    },
    PayloadTopic = payload_topic(ClientID, MsgKey),
    PayloadBin = pack_payload(Payload, Ts),
    TxFun = fun() ->
        emqx_ds:tx_del_topic(PayloadTopic),
        emqx_ds:tx_write(PayloadTopic, ?ds_tx_ts_monotonic, PayloadBin)
    end,
    case emqx_ds:trans(TxOpts, TxFun) of
        {Ref, ok} ->
            {ok, Ref};
        {error, Recoverable, Reason} ->
            % TODO: retry
            {error, {Recoverable, Reason}}
    end.

payload_topic(ClientID, MsgKey) ->
    [?payload, ClientID, ?key, MsgKey].

pack_payload(Payload, Ts) ->
    %% TODO
    %% pack with ASN.1
    term_to_binary({Payload, Ts}).

unpack_payload(Payload) ->
    %% TODO
    {_Payload, _Ts} = binary_to_term(Payload).

%% @doc Ack and fetch a batch of messages starting from the given seqno.
-spec ack_and_fetch_next_batch(ClientID :: binary(), no_ack | seqno(), Limit :: non_neg_integer()) ->
    [msg()].
ack_and_fetch_next_batch(ClientID, LastSeqno0, Limit) ->
    LastSeqno =
        case is_integer(LastSeqno0) of
            true ->
                ok = mark_acked(ClientID, LastSeqno0),
                LastSeqno0;
            false ->
                #?STATE_REC{acked = Acked} = read_state(ClientID),
                Acked
        end,
    fetch_batch_loop(ClientID, LastSeqno, Limit, []).

%% @private Fetch messages to be sent to a consumer.
fetch_batch_loop(_ClientID, _LastSeqno, 0, Acc) ->
    lists:reverse(Acc);
fetch_batch_loop(ClientID, LastSeqno, Limit, Acc) ->
    case mnesia:dirty_next(?INDEX_TAB, ?INDEX_KEY(ClientID, LastSeqno)) of
        ?INDEX_KEY(ClientID, NextSeqno) = IndexKey ->
            case mnesia:dirty_read(?INDEX_TAB, IndexKey) of
                [] ->
                    %% The index is deleted due to race condition.
                    %% May happen due to lack of atomicity of the transaction.
                    fetch_batch_loop(ClientID, NextSeqno, Limit, Acc);
                [#?INDEX_REC{msg_key = MsgKey, ts = Ts}] ->
                    case
                        mnesia:dirty_read(
                            ?PAYLOAD_TAB, ?PAYLOAD_KEY(ClientID, MsgKey, NextSeqno, Ts)
                        )
                    of
                        [] ->
                            %% The payload is deleted due to race condition.
                            %% May happen due to lack of atomicity of the transaction.
                            fetch_batch_loop(ClientID, NextSeqno, Limit, Acc);
                        [#?PAYLOAD_REC{payload = Payload}] ->
                            NewAcc = [
                                #{seqno => NextSeqno, msg_key => MsgKey, payload => Payload} | Acc
                            ],
                            fetch_batch_loop(ClientID, NextSeqno, Limit - 1, NewAcc)
                    end
            end;
        _ ->
            lists:reverse(Acc)
    end.

%% @doc Acknowledge messages before the given seqno.
-spec mark_acked(ClientID :: binary(), Seqno :: seqno()) -> ok.
mark_acked(ClientID, Seqno) ->
    State = read_state(ClientID),
    NewState = State#?STATE_REC{acked = Seqno, last_ack_ts = now_ts()},
    mria:dirty_write(?STATE_TAB, NewState).

%% @private Traverse backward to find and delete the payload records for the given payload key (if any).
%% Returns the list of deleted sequence numbers.
delete_old_payload(PayloadKey) ->
    delete_old_payload(PayloadKey, []).

delete_old_payload(?PAYLOAD_KEY(ClientID, MsgKey, _Seqno, _) = Key, Acc) ->
    case mnesia:dirty_prev(?PAYLOAD_TAB, Key) of
        ?PAYLOAD_KEY(ClientID, MsgKey, PrevSeqno, _) = PrevKey ->
            delete_old_payload(PrevKey, [PrevSeqno | Acc]);
        _ ->
            lists:reverse(Acc)
    end.

%% @private Delete the index records (after payload is deleted).
delete_index(_ClientID, []) ->
    ok;
delete_index(ClientID, [Seqno | Seqnos]) ->
    mria:dirty_delete(?INDEX_TAB, ?INDEX_KEY(ClientID, Seqno)),
    delete_index(ClientID, Seqnos).

read_state(ClientID) ->
    case mnesia:dirty_read(?STATE_TAB, ClientID) of
        [State] ->
            State;
        [] ->
            State = new_state(ClientID),
            mria:dirty_write(?STATE_TAB, State),
            State
    end.

new_state(ClientID) ->
    #?STATE_REC{clientid = ClientID}.

now_ts() ->
    erlang:system_time(millisecond).

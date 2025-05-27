%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ecq_store).

%% Bootstrapping
-export([
    create_tables/0
]).

%% Runtime
-export([
    append/4,
    ack_and_fetch_next_batch/3,
    mark_acked/2,
    gc/2
]).

%% Observability
-export([
    status/0
]).

-export_type([msg/0]).

-include("emqx_ecq.hrl").
-define(EOT, '$end_of_table').
-define(MIN_SEQNO, 0).
-define(MAX_SEQNO, 1 bsl 64).
-define(MIN_MSG_KEY, <<>>).
-define(MAX_MSG_KEY, <<255>>).

-type msg() :: #{seqno := seqno(), msg_key := msg_key(), payload := binary()}.

%% @doc Create the tables.
create_tables() ->
    ok = mria:create_table(?SEQNO_TAB, [
        {type, set},
        {rlog_shard, ?DB_SHARD},
        {storage, disc_copies},
        {record_name, ?SEQNO_REC},
        {attributes, record_info(fields, ?SEQNO_REC)}
    ]),
    ok = mria:create_table(?STATE_TAB, [
        {type, ordered_set},
        {rlog_shard, ?DB_SHARD},
        {storage, disc_copies},
        {record_name, ?STATE_REC},
        {attributes, record_info(fields, ?STATE_REC)}
    ]),
    ok = mria:create_table(?INDEX_TAB, [
        {type, ordered_set},
        {rlog_shard, ?DB_SHARD},
        {storage, disc_copies},
        {record_name, ?INDEX_REC},
        {attributes, record_info(fields, ?INDEX_REC)}
    ]),
    ok = mria:create_table(?PAYLOAD_TAB, [
        {type, ordered_set},
        {rlog_shard, ?DB_SHARD},
        {storage, rocksdb_copies},
        {record_name, ?PAYLOAD_REC},
        {attributes, record_info(fields, ?PAYLOAD_REC)}
    ]),
    ok = mria:wait_for_tables([?SEQNO_TAB, ?INDEX_TAB, ?PAYLOAD_TAB]).

status() ->
    #{
        memory_usage => memory_usage(),
        total_messages => mnesia:table_info(?INDEX_TAB, size),
        consumers => consumers()
    }.

consumers() ->
    #{
        state => mnesia:table_info(?STATE_TAB, size),
        seqno => mnesia:table_info(?SEQNO_TAB, size)
    }.

memory_usage() ->
    WordSize = erlang:system_info(wordsize),
    #{
        index => mnesia:table_info(?INDEX_TAB, memory) * WordSize,
        state => mnesia:table_info(?STATE_TAB, memory) * WordSize,
        seqno => mnesia:table_info(?SEQNO_TAB, memory) * WordSize
    }.

%% @doc Append a new message to a queue.
%% 1. Allocate a new sequence number.
%% 2. Insert the message into the index table.
%% 3. Insert the message into the payload table.
%% 4. Initialize the state if not exists.
%% NOTE: all operations are dirty as intended, transactions are costly.
%% TODO: add an option for transactional append.
-spec append(ClientID :: binary(), MsgKey :: binary(), Payload :: binary(), Ts :: ts()) ->
    {ok, seqno()}.
append(ClientID, MsgKey, Payload, Ts) ->
    %% assign a new sequence number
    Seqno = mria:dirty_update_counter(?SEQNO_TAB, ClientID, 1),
    %% Insert index
    Index = #?INDEX_REC{key = ?INDEX_KEY(ClientID, Seqno), msg_key = MsgKey, ts = Ts},
    ok = mria:dirty_write(?INDEX_TAB, Index),
    %% Insert payload
    PayloadKey = ?PAYLOAD_KEY(ClientID, MsgKey, Seqno, Ts),
    PayloadRec = #?PAYLOAD_REC{key = PayloadKey, payload = Payload},
    ok = mria:dirty_write(?PAYLOAD_TAB, PayloadRec),
    DeletedSeqnos = delete_old_payload(PayloadKey),
    ok = delete_index(ClientID, DeletedSeqnos),
    {ok, Seqno}.

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

%% @private Garbage collect the index and payload records for the given clientid.
%% Returns `complete' if the clientid is the last one, or `{continue, ClientID}' otherwise.
gc(PrevClientID, ExpireAt) ->
    ClientID = next_clientid(PrevClientID),
    case ClientID =:= ?EOT of
        true ->
            complete;
        false ->
            ok = run_gc_index_and_payload(ClientID, ExpireAt),
            {continue, ClientID}
    end.

%% Make a index table key that is smaller than the given clientid.
%% It's next is guaranteed to be the first key of the given clientid (if exists).
min_index_key(ClientID) ->
    ?INDEX_KEY(ClientID, ?MIN_SEQNO).

%% Make a payload table key that is smaller than the given clientid.
%% It's next is guaranteed to be the first key of the given clientid (if exists).
min_payload_key(ClientID) ->
    ?PAYLOAD_KEY(ClientID, ?MIN_MSG_KEY, 0, 0).

%% Make a index table key that is smaller than the given clientid.
%% It's next is guaranteed to be the next clientid.
max_index_key(PrevClientID) ->
    ?INDEX_KEY(PrevClientID, ?MAX_SEQNO).

%% Make a payload table key that is smaller than the given clientid.
%% It's next is guaranteed to be the next clientid.
max_payload_key(PrevClientID) ->
    ?PAYLOAD_KEY(PrevClientID, ?MAX_MSG_KEY, 0, 0).

next_clientid(PrevClientID) ->
    Id1 = mnesia:dirty_next(?INDEX_TAB, max_index_key(PrevClientID)),
    Id2 = mnesia:dirty_next(?PAYLOAD_TAB, max_payload_key(PrevClientID)),
    min_clientid([Id1, Id2], ?EOT).

min_clientid([], Min) ->
    Min;
min_clientid([?EOT | Rest], Min) ->
    min_clientid(Rest, Min);
min_clientid([Key | Rest], Min) ->
    ClientID =
        case Key of
            ?INDEX_KEY(ID, _) ->
                ID;
            ?PAYLOAD_KEY(ID, _, _, _) ->
                ID
        end,
    case is_smaller(ClientID, Min) of
        true ->
            min_clientid(Rest, ClientID);
        false ->
            min_clientid(Rest, Min)
    end.

is_smaller(_, ?EOT) ->
    true;
is_smaller(Id1, Id2) ->
    Id1 < Id2.

%% @private Deletes the expired index and payload records.
run_gc_index_and_payload(ClientID, ExpireAt) ->
    ok = gc_index(ClientID, ExpireAt),
    ok = gc_payload(ClientID, ExpireAt),
    ok.

gc_index(ClientID, ExpireAt) ->
    First = mnesia:dirty_next(?INDEX_TAB, min_index_key(ClientID)),
    gc_index_loop(ClientID, First, ExpireAt).

gc_index_loop(ClientID, ?INDEX_KEY(ClientID, _) = Key, ExpireAt) ->
    case mnesia:dirty_read(?INDEX_TAB, Key) of
        [#?INDEX_REC{ts = Ts}] when Ts < ExpireAt ->
            mnesia:dirty_delete(?INDEX_TAB, Key),
            gc_index_loop(ClientID, mnesia:dirty_next(?INDEX_TAB, Key), ExpireAt);
        _ ->
            %% Stop looping here because the rest are likely not expired,
            %% since they are inserted sequentially.
            ok
    end;
gc_index_loop(_, _, _) ->
    %% Another clientid, or the end of the table.
    ok.

gc_payload(ClientID, ExpireAt) ->
    First = mnesia:dirty_next(?PAYLOAD_TAB, min_payload_key(ClientID)),
    gc_payload_loop(ClientID, First, ExpireAt).

gc_payload_loop(ClientID, ?PAYLOAD_KEY(ClientID, _, _, Ts) = Key, ExpireAt) when Ts < ExpireAt ->
    mnesia:dirty_delete(?PAYLOAD_TAB, Key),
    gc_payload_loop(ClientID, mnesia:dirty_next(?PAYLOAD_TAB, Key), ExpireAt);
gc_payload_loop(_, _, _) ->
    %% Stop looping due to:
    %% 1. Another clientid, or the end of the table.
    %% 2. The key is not expired (and the rest, if any, are likely not expired).
    ok.

-ifndef(EMQX_ECQ_HRL).
-define(EMQX_ECQ_HRL, true).

-define(PLUGIN_NAME, "emqx_ecq").
-define(PLUGIN_VSN, ?plugin_rel_vsn).
-define(PLUGIN_NAME_VSN, <<?PLUGIN_NAME, "-", ?PLUGIN_VSN>>).

%% mnesia
-define(SEQNO_TAB, ecq_seqno).
-define(STATE_TAB, ecq_state).
-define(INDEX_TAB, ecq_index).
-define(PAYLOAD_TAB, ecq_payload).
%% ets
-define(INFLIGHT_TAB, ecq_inflight).

%% records
-define(SEQNO_REC, ?SEQNO_TAB).
-define(STATE_REC, ?STATE_TAB).
-define(INDEX_REC, ?INDEX_TAB).
-define(PAYLOAD_REC, ?PAYLOAD_TAB).

-type seqno() :: non_neg_integer().
-type clientid() :: binary().

-record(?SEQNO_REC, {
    clientid :: clientid(),
    %% the next sequence number to be sent to the consumer
    next = 0 :: seqno()
}).

-record(?STATE_REC, {
    clientid :: clientid(),
    %% the last seqno acknowledged
    acked = 0 :: seqno(),
    %% the last acknolegment timestamp, used for gc
    last_ack_ts = 0 :: ts(),
    %% extra state
    extra = [] :: [any()]
}).

-define(INDEX_KEY(ClientID, SeqNo), {ClientID, SeqNo}).
-type index_key() :: ?INDEX_KEY(clientid(), seqno()).
-type msg_key() :: binary().
-type ts() :: erlang:timestamp().
-record(?INDEX_REC, {
    key :: index_key(),
    msg_key :: msg_key(),
    ts :: ts()
}).

-define(PAYLOAD_KEY(ClientID, MsgKey, SeqNo, Ts), {ClientID, MsgKey, SeqNo, Ts}).
-type payload_key() :: ?PAYLOAD_KEY(clientid(), msg_key(), seqno(), ts()).
-record(?PAYLOAD_REC, {
    key :: payload_key(),
    payload :: binary()
}).

-define(DB_SHARD, ecq_shard).

-define(DS_DB, ecq).

-define(WRITER_POOL, emqx_ecq_writer).
-define(READER_REG_POOL, emqx_ecq_reader_reg).

-include_lib("emqx_plugin_helper/include/logger.hrl").
-define(LOG(Level, Msg, Data),
    ?SLOG(Level, maps:merge(Data, #{msg => Msg, tag => "ECQ"}))
).
-define(ROLE_PT_KEY, ecq_node_role).

-define(READER_PD_KEY, ecq_reader).
-define(NO_ACK, no_ack).
-define(HEARTBEAT_POLL_INTERVAL, timer:seconds(30)).
%% Default batch size for polling.
%% this value should be smaller than MQTT inflight window.
%% so to ensure non-ECQ messages get fair chance to be delivered.
-define(POLL_BATCH_SIZE, 10).
-endif.

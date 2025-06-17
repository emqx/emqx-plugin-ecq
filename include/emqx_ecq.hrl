-ifndef(EMQX_ECQ_HRL).
-define(EMQX_ECQ_HRL, true).

-define(PLUGIN_NAME, "emqx_ecq").
-define(PLUGIN_VSN, ?plugin_rel_vsn).
-define(PLUGIN_NAME_VSN, <<?PLUGIN_NAME, "-", ?PLUGIN_VSN>>).

-type seqno() :: non_neg_integer().
-type clientid() :: binary().
-type msg_key() :: binary().
-type ts() :: erlang:timestamp().

%% ets
-define(INFLIGHT_TAB, ecq_inflight).

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

%% DS
-define(payload, <<"payload">>).
-define(key, <<"msg_key">>).
-define(ds_tx_ts_monotonic, tx_ts_monotonic).

-endif.

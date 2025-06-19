%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Reader is a process linked to the subscriber process.
-module(emqx_ecq_reader).

-export([start_link/2]).

-export([
    subscribed/1,
    acked/1,
    heartbeat/0,
    notify/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    handle_continue/2,
    terminate/2,
    code_change/3
]).

-include("emqx_ecq.hrl").
-include_lib("emqx_plugin_helper/include/emqx_message.hrl").

-record(poll_notification, {}).

-define(LAST_HEARTBEAT_TS, ecq_last_heartbeat_ts).

%% @doc Start one agent process.
start_link(ClientID, SubPid) ->
    gen_server:start_link(
        ?MODULE,
        [ClientID, SubPid],
        [{hibernate_after, 1000}]
    ).

%% @doc Notify the agent that a consumer subscribed to the queue.
-spec subscribed(clientid()) -> ok.
subscribed(ClientID) ->
    ReaderPid = ensure_reader(ClientID),
    notify_poll(ReaderPid).

ensure_reader(ClientID) ->
    case get_reader_pd() of
        undefined ->
            {ok, ReaderPid} = start_link(ClientID, self()),
            ok = new_reader_pd(ClientID, ReaderPid),
            ok = emqx_ecq_reader_reg:add(self(), ReaderPid),
            ?LOG(debug, "reader_registered", #{reader => ReaderPid}),
            ReaderPid;
        #{pid := ReaderPid} ->
            ReaderPid
    end.

%% @doc Acknowledge messages up to Seqno.
%% Read a batch of messages from the queue and send them to the consumer pid.
-spec acked(seqno()) -> ok.
acked(Seqno) ->
    SubPid = self(),
    ok = update_reader_pd(Seqno),
    ok = emqx_ecq_inflight:delete(SubPid, Seqno),
    case emqx_ecq_inflight:exists(SubPid) of
        true ->
            %% there are more messages in the inflight table
            %% keep waiting until all is acked
            ok;
        false ->
            %% then notify the agent to read a new batch.
            notify_poll()
    end.

%% @doc Heartbeat to ensure messages are eventually delivered,
%% even if some notifications were previously missed due to race conditions.
-spec heartbeat() -> ok.
heartbeat() ->
    case get_reader_pd() of
        undefined ->
            %% not a ECQ consumer
            ok;
        #{last_ack_ts := LastTs, clientid := ClientID} ->
            ?LOG(debug, "heartbeat_maybe_trigger", #{clientid => ClientID}),
            SubPid = self(),
            heartbeat2(SubPid, LastTs, ClientID)
    end.

heartbeat2(SubPid, LastTs, ClientID) ->
    HbTs = get(?LAST_HEARTBEAT_TS),
    MaxTs =
        case HbTs of
            undefined ->
                LastTs;
            _ ->
                max(HbTs, LastTs)
        end,
    case now_ts() - MaxTs > ?HEARTBEAT_POLL_INTERVAL of
        true ->
            heartbeat3(SubPid, ClientID);
        false ->
            ok
    end.

heartbeat3(SubPid, ClientID) ->
    case emqx_ecq_inflight:exists(SubPid) of
        true ->
            %% There are inflight messages, wait for ack to trigger the next poll.
            ok;
        false ->
            put(?LAST_HEARTBEAT_TS, now_ts()),
            ?LOG(debug, "heartbeat_triggered", #{clientid => ClientID}),
            notify_poll()
    end.

notify_poll() ->
    #{pid := ReaderPid} = get_reader_pd(),
    notify_poll(ReaderPid).

notify_poll(ReaderPid) ->
    gen_server:cast(ReaderPid, #poll_notification{}).

%% gen_server callbacks
init([ClientID, SubPid]) ->
    process_flag(trap_exit, true),
    emqx_logger:set_proc_metadata(#{
        clientid => ClientID
    }),
    case emqx_ecq_store:init_read_state(ClientID) of
        {ok, ReadState} ->
            State = #{sub_pid => SubPid, read_state => ReadState, clientid => ClientID},
            {ok, State};
        {error, Error} ->
            {stop, Error}
    end.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(
    #poll_notification{}, #{sub_pid := SubPid} = State0
) ->
    %% TODO
    %% Can data race happen here?
    State =
        case emqx_ecq_inflight:exists(SubPid) of
            true ->
                %% there are inflight messages, wait for ack to trigger the next poll.
                State0;
            false ->
                handle_poll(State0)
        end,
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

handle_continue(_Continue, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

now_ts() ->
    erlang:system_time(millisecond).

%% @doc Notify local subscribers of new data.
notify(ClientID) ->
    case get_online_sub_pids(ClientID) of
        [] ->
            ?LOG(debug, "no_subscriber_online", #{subscriber => ClientID}),
            ok;
        [SubPid] ->
            case emqx_ecq_inflight:exists(SubPid) of
                true ->
                    %% there are inflight messages, wait for ack to trigger the next poll.
                    ?LOG(debug, "inflight_messages_will_delay_notification", #{
                        subscriber => ClientID
                    }),
                    ok;
                false ->
                    case emqx_ecq_reader_reg:lookup(SubPid) of
                        [] ->
                            ?LOG(debug, "no_reader_registered", #{subscriber => ClientID}),
                            ok;
                        [ReaderPid] ->
                            ?LOG(debug, "notify_reader", #{subscriber => ClientID}),
                            notify_poll(ReaderPid)
                    end
            end;
        _ ->
            %% notification will be triggered when the connection is stable.
            ?LOG(warning, "multiple_subscribers_online__delay_notification", #{
                subscriber => ClientID
            })
    end.

get_online_sub_pids(ClientID) ->
    SubPids = emqx_broker:subscribers(sub_topic(ClientID)),
    lists:filtermap(
        fun(Pid) ->
            case emqx_cm:is_channel_connected(Pid) of
                true -> {true, Pid};
                false -> false
            end
        end,
        SubPids
    ).

sub_topic(ClientID) ->
    <<"$ECQ/", ClientID/binary, "/#">>.

pub_topic(ClientID, MsgKey) ->
    <<"$ECQ/", ClientID/binary, "/", MsgKey/binary>>.

handle_poll(#{clientid := ClientID, sub_pid := SubPid, read_state := ReadState0} = State) ->
    BatchSize = emqx_ecq_config:get_reader_batch_size(),
    {Time, Res} = timer:tc(fun() ->
        emqx_ecq_store:ack_and_fetch_next_batch(ReadState0, BatchSize)
    end),
    case Res of
        {ok, Batch, ReadState} ->
            ?LOG(debug, "succeeded_to_fetch_next_batch", #{
                batch_size => length(Batch), time_us => Time
            }),
            ok = deliver_batch(ClientID, SubPid, Batch),
            State#{read_state := ReadState};
        {error, Reason} ->
            ?LOG(error, "failed_to_fetch_next_batch", #{reason => Reason, time_us => Time}),
            State
    end.

deliver_batch(_ClientID, _SubPid, []) ->
    ?LOG(debug, "no_message_to_forward", #{batch_size => 0}),
    ok;
deliver_batch(ClientID, SubPid, Msgs) ->
    ?LOG(debug, "fowarding_a_batch_of_messages", #{batch_size => length(Msgs)}),
    Inflights = lists:map(fun(#{seqno := Seqno}) -> {SubPid, Seqno} end, Msgs),
    ok = emqx_ecq_inflight:insert(Inflights),
    lists:foreach(
        fun(Msg) ->
            deliver_to_subscriber(SubPid, ClientID, Msg)
        end,
        Msgs
    ),
    ok.

deliver_to_subscriber(SubPid, ClientID, Msg) ->
    #{seqno := Seqno, msg_key := MsgKey, payload := Payload, ts := Ts} = Msg,
    Topic = pub_topic(ClientID, MsgKey),
    Message = make_message(Topic, Payload, Seqno, Ts),
    ok = emqx_ecq_metrics:observe_delivery_latency(forward, Ts),
    _ = erlang:send(SubPid, {deliver, Topic, Message}),
    ?LOG(debug, "message_forwarded_to_subscriber", #{
        msg_key => MsgKey,
        payload_size => byte_size(Payload)
    }),
    ok.

get_reader_pd() ->
    erlang:get(?READER_PD_KEY).

new_reader_pd(ClientID, ReaderPid) ->
    PD = #{pid => ReaderPid, seqno => ?NO_ACK, last_ack_ts => now_ts(), clientid => ClientID},
    put(?READER_PD_KEY, PD),
    ok.

update_reader_pd(Seqno) ->
    PD = get_reader_pd(),
    PD2 = PD#{seqno := Seqno, last_ack_ts := now_ts()},
    put(?READER_PD_KEY, PD2),
    ok.

make_message(Topic, Payload, Seqno, Ts) ->
    From = <<"ECQ">>,
    Qos = 1,
    Headers = make_headers(Seqno),
    Flags = #{},
    Message = emqx_message:make(From, Qos, Topic, Payload, Flags, Headers),
    Message#message{timestamp = Ts}.

make_headers(Seqno) ->
    Props = #{'User-Property' => [{<<"ecq-seqno">>, integer_to_binary(Seqno)}]},
    #{properties => Props}.

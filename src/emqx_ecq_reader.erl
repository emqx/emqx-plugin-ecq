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

%% RPC handler
-export([handle_poll_local/2]).

-include("emqx_ecq.hrl").

-record(poll_notification, {
    last_acked :: seqno()
}).

-type msg() :: emqx_ecq_store:msg().

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
    notify_poll(ReaderPid, ?NO_ACK).

ensure_reader(ClientID) ->
    case get_reader_pd() of
        undefined ->
            {ok, ReaderPid} = start_link(ClientID, self()),
            ok = new_reader_pd(ReaderPid),
            ok = emqx_ecq_reader_reg:add(self(), ReaderPid),
            ?LOG(debug, "reader_registered", #{reader => ReaderPid}),
            ReaderPid;
        ReaderPid ->
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
            notify_poll(Seqno)
    end.

%% @doc Heartbeat to ensure messages are eventually delivered,
%% even if some notifications were previously missed due to race conditions.
-spec heartbeat() -> ok.
heartbeat() ->
    case get_reader_pd() of
        undefined ->
            %% not a ECQ consumer
            ok;
        #{seqno := LastAcked, last_ack_ts := LastTs} ->
            SubPid = self(),
            heartbeat2(SubPid, LastAcked, LastTs)
    end.

heartbeat2(SubPid, LastAcked, LastTs) ->
    case now_ts() - LastTs > ?HEARTBEAT_POLL_INTERVAL of
        true ->
            heartbeat3(SubPid, LastAcked);
        false ->
            ok
    end.

heartbeat3(SubPid, LastAcked) ->
    case emqx_ecq_inflight:exists(SubPid) of
        true ->
            %% There is inflight messages, wait for ack to trigger the next poll.
            ok;
        false ->
            notify_poll(LastAcked)
    end.

notify_poll(LastAcked) ->
    #{pid := ReaderPid} = get_reader_pd(),
    notify_poll(ReaderPid, LastAcked).

notify_poll(ReaderPid, LastAcked) ->
    gen_server:cast(ReaderPid, #poll_notification{last_acked = LastAcked}).

%% gen_server callbacks
init([ClientID, SubPid]) ->
    process_flag(trap_exit, true),
    emqx_logger:set_proc_metadata(#{
        clientid => ClientID
    }),
    {ok, #{clientid => ClientID, sub_pid => SubPid}}.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(
    #poll_notification{last_acked = LastAcked}, #{sub_pid := SubPid} = State
) ->
    case emqx_ecq_inflight:exists(SubPid) of
        true ->
            %% there are inflight messages, wait for ack to trigger the next poll.
            ok;
        false ->
            handle_poll_notification(LastAcked, State)
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
                            notify_poll(ReaderPid, ?NO_ACK)
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

handle_poll_notification(LastAcked, #{clientid := ClientID, sub_pid := SubPid}) ->
    Batch =
        case emqx_ecq_config:my_role() of
            core ->
                handle_poll_local(ClientID, LastAcked);
            _ ->
                case emqx_ecq_writer_dist:pick_core_node(ClientID) of
                    {ok, Node} ->
                        erpc:call(Node, ?MODULE, handle_poll_local, [ClientID, LastAcked]);
                    {error, no_running_core_nodes} ->
                        ?LOG(warning, "no_running_core_nodes", #{})
                end
        end,
    ok = deliver_batch(ClientID, SubPid, Batch).

%% @doc Handle poll notification on the core node.
-spec handle_poll_local(clientid(), ?NO_ACK | seqno()) -> [msg()].
handle_poll_local(ClientID, LastAcked) ->
    BatchSize = emqx_ecq_config:get_reader_batch_size(),
    emqx_ecq_store:ack_and_fetch_next_batch(ClientID, LastAcked, BatchSize).

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
    #{seqno := Seqno, msg_key := MsgKey, payload := Payload} = Msg,
    Topic = pub_topic(ClientID, MsgKey),
    Message = make_message(Topic, Payload, Seqno),
    _ = erlang:send(SubPid, {deliver, Topic, Message}),
    ?LOG(debug, "message_forwarded_to_subscriber", #{
        msg_key => MsgKey,
        payload_size => byte_size(Payload)
    }),
    ok.

get_reader_pd() ->
    erlang:get(?READER_PD_KEY).

new_reader_pd(ReaderPid) ->
    PD = #{pid => ReaderPid, seqno => ?NO_ACK, last_ack_ts => now_ts()},
    put(?READER_PD_KEY, PD),
    ok.

update_reader_pd(Seqno) ->
    PD = get_reader_pd(),
    PD2 = PD#{seqno := Seqno, last_ack_ts := now_ts()},
    put(?READER_PD_KEY, PD2),
    ok.

make_message(Topic, Payload, Seqno) ->
    From = <<"ECQ">>,
    Qos = 1,
    Headers = make_headers(Seqno),
    Flags = #{},
    emqx_message:make(From, Qos, Topic, Payload, Flags, Headers).

make_headers(Seqno) ->
    Props = #{'User-Property' => [{<<"ecq-seqno">>, integer_to_binary(Seqno)}]},
    #{properties => Props}.

%% This test suite verifies the plugin as a blackbox.
%% It assumes that the broker with the plugin installed
%% is running on endpoint defined in environment variable
%% EMQX_MQTT_ENDPOINT.
-module(emqx_ecq_integration_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

%%--------------------------------------------------------------------
%% CT boilerplate
%%--------------------------------------------------------------------

all() ->
    emqx_ecq_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% Consumer connect and subscribe, message is published while the consumer is online.
t_realtime_dispatch(_Config) ->
    SubClientID = random_clientid("sub-"),
    {ok, SubPid} = start_subscriber(SubClientID),
    {ok, PubPid} = start_publisher(),
    Key = <<"key/1">>,
    {ok, DataToExpect} = publish_data(PubPid, SubClientID, Key),
    ok = assert_exact_payload_received(SubPid, SubClientID, Key, DataToExpect),
    ok = stop_client(PubPid),
    ok = stop_client(SubPid).

%% A message is published by publisher, then a subscriber connects and subscribes.
t_late_subscribe(_Config) ->
    SubClientID = random_clientid("sub-"),
    Key = <<"key/1">>,
    {ok, PubPid} = start_publisher(),
    {ok, Data} = publish_data(PubPid, SubClientID, Key),
    {ok, SubPid} = start_subscriber(SubClientID),
    ok = assert_exact_payload_received(SubPid, SubClientID, Key, Data),
    ok = stop_client(SubPid),
    ok = stop_client(PubPid).

t_direct_publish_to_ecq_topic_is_not_allowed(_Config) ->
    {ok, PubPid} = start_publisher(),
    SubClientID = random_clientid("sub-"),
    {ok, SubPid} = start_subscriber(SubClientID),
    Key = <<"key/1">>,
    %% This publish should not be sent to the subscriber (terminated by the plugin).
    {ok, _} = emqtt:publish(PubPid, bin(["$ECQ/", SubClientID, "/", Key]), <<"data1">>, 1),
    %% This publish should be sent to the subscriber.
    {ok, _} = emqtt:publish(PubPid, bin(["normal/", SubClientID, "/", Key]), <<"data2">>, 1),
    receive
        {publish_received, SubPid, SubClientID, Msg} ->
            ?assertMatch(#{qos := 1}, Msg),
            ?assertEqual(<<"data2">>, maps:get(payload, Msg)),
            Topic = maps:get(topic, Msg),
            ExpectedTopic = bin(["normal/", SubClientID, "/", Key]),
            ?assertEqual(ExpectedTopic, Topic)
    after 10000 ->
        ct:fail(#{reason => timeout})
    end,
    receive
        {publish_received, SubPid, SubClientID, _Msg} ->
            ct:fail(#{reason => "should not reach here"})
    after 500 ->
        ok
    end,
    ok = stop_client(PubPid),
    ok = stop_client(SubPid).

%% A subscriber disconnects, then reconnects with session persisted.
%% It will not queue the message in its session state while it is offline.
t_disconnected_session_does_not_queue_messages(_Config) ->
    %% TODO: start two subs, one nomral, one ECQ
    %% inspect the mqueue state of both subs
    ok.

t_compaction(_Config) ->
    {ok, PubPid} = start_publisher(),
    SubClientID = random_clientid("sub-"),
    Key = <<"key/1">>,
    lists:foreach(
        fun(_) ->
            {ok, _Data} = publish_data(PubPid, SubClientID, Key)
        end,
        lists:seq(1, 100)
    ),
    {ok, Data} = publish_data(PubPid, SubClientID, Key),
    {ok, SubPid} = start_subscriber(SubClientID),
    ok = assert_exact_payload_received(SubPid, SubClientID, Key, Data),
    ok = stop_client(PubPid),
    ok = stop_client(SubPid).

%% Reconnected client should not receive already received messages,
%% should receive only new messages.
t_connection_restoration(_Config) ->
    SubClientID = random_clientid("sub-"),
    {ok, SubPid0} = start_subscriber(SubClientID),
    Key = <<"key/1">>,
    {ok, PubPid} = start_publisher(),
    {ok, Data0} = publish_data(PubPid, SubClientID, Key),
    ok = assert_exact_payload_received(SubPid0, SubClientID, Key, Data0),
    ok = stop_client(SubPid0),
    {ok, SubPid1} = start_subscriber(SubClientID),
    ok = assert_not_received(SubPid1, SubClientID, Key),
    {ok, Data1} = publish_data(PubPid, SubClientID, Key),
    ok = assert_exact_payload_received(SubPid1, SubClientID, Key, Data1),
    ok = stop_client(PubPid),
    ok = stop_client(SubPid1).

t_metrics_format(_Config) ->
    meck:new(emqx_ecq_metrics, [passthrough]),
    meck:expect(emqx_ecq_metrics, get_metrics, fun(_) ->
        #{
            counters => #{},
            rate => #{},
            gauges => #{},
            hists =>
                #{
                    append =>
                        #{
                            count => 1754,
                            sum => 266440,
                            bucket_counts =>
                                [
                                    {10, 0},
                                    {20, 0},
                                    {50, 111},
                                    {100, 209},
                                    {200, 1401},
                                    {500, 1754},
                                    {1000, 1754},
                                    {2000, 1754},
                                    {5000, 1754},
                                    {10000, 1754},
                                    {20000, 1754},
                                    {infinity, 1754}
                                ]
                        }
                },
            slides => #{}
        }
    end),
    IoData = emqx_ecq_metrics:format_hist(ds_latency, append),
    ct:print("~s", [IoData]),
    meck:unload(emqx_ecq_metrics).

%% Publish many messages to many keys.
t_massive_publishes(_Config) ->
    %% Parameters.
    NKeys = 100,
    NMessages = 50,

    %% Init.
    ct:timetrap(timer:seconds(180)),
    SubClientID = random_clientid("sub-"),
    {ok, SubPid} = start_subscriber(SubClientID),
    Self = self(),

    %% Function to publish N messages for a given key asynchronously.
    PubN = fun(Key, N) ->
        spawn_link(fun() ->
            {ok, PubPid} = start_publisher(),
            Payloads = lists:map(
                fun(_) ->
                    {ok, Data} = publish_data(PubPid, SubClientID, Key),
                    Data
                end,
                lists:seq(1, N)
            ),
            ok = stop_client(PubPid),
            Self ! {payloads_published, Key, lists:last(Payloads)}
        end)
    end,

    %% Generate 100 keys to publish to.
    Keys = lists:map(fun(N) -> <<"key/", (integer_to_binary(N))/binary>> end, lists:seq(1, NKeys)),

    %% Publish 100 messages for each key.
    lists:foreach(
        fun(Key) ->
            PubN(Key, NMessages)
        end,
        Keys
    ),

    %% Collect last message for each key.
    KeyData = lists:map(
        fun(_Key) ->
            receive
                {payloads_published, Key, Data} ->
                    {Key, Data}
            end
        end,
        Keys
    ),

    ct:print("All messages published, waiting for subscriber to receive them."),
    %% Assert that the subscriber received the last message for each key.
    lists:foreach(
        fun({Key, Data}) ->
            ok = assert_payload_received(SubPid, SubClientID, Key, Data)
        end,
        KeyData
    ),

    %% Print some stat.
    N = drain_messages(SubPid, SubClientID) + NKeys,
    ct:print("Received ~p messages from ~p", [N, NKeys * NMessages]),

    %% Cleanup.
    ok = stop_client(SubPid).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

%% Assert that the subscriber received the exact message for a given key.
%% Messages to other clients, topics, queues, or keys are an error.
assert_exact_payload_received(SubPid, SubClientID, Key, Data) ->
    ExpectedTopic = bin(["$ECQ/", SubClientID, "/", Key]),
    receive
        {publish_received, SubPid, SubClientID, #{
            qos := 1, topic := ExpectedTopic, payload := Data
        }} ->
            ok;
        {publish_received, OtherSubPid, OtherSubClientID, OtherMsg} ->
            ct:fail(#{
                reason => "unexpected publish received",
                msg => OtherMsg,
                key => Key,
                pid => OtherSubPid,
                sub_clientid => OtherSubClientID
            })
    after 10000 ->
        ct:fail(#{reason => timeout})
    end.

%% Assert that the subscriber received the message for a given key.
%% Messages to other clients, topics, queues, or keys are ignored.
assert_payload_received(SubPid, SubClientID, Key, Data) ->
    ExpectedTopic = bin(["$ECQ/", SubClientID, "/", Key]),
    receive
        {publish_received, SubPid, SubClientID, #{
            qos := 1, topic := ExpectedTopic, payload := Data
        }} ->
            ok
    after 10000 ->
        ct:fail(#{reason => timeout})
    end.

drain_messages(SubPid, SubClientID) ->
    drain_messages(SubPid, SubClientID, 0).

drain_messages(SubPid, SubClientID, N) ->
    receive
        {publish_received, SubPid, SubClientID, _Msg} ->
            drain_messages(SubPid, SubClientID, N + 1)
    after 500 ->
        N
    end.

assert_not_received(SubPid, SubClientID, Key) ->
    Topic = bin(["$ECQ/", SubClientID, "/", Key]),
    receive
        {publish_received, SubPid, SubClientID, #{topic := Topic} = Msg} ->
            ct:fail(#{
                reason => "should not receive message under key",
                topic => Topic,
                msg => Msg,
                key => Key,
                sub_clientid => SubClientID
            })
    after 500 ->
        ok
    end.

start_publisher() ->
    ClientID = random_clientid("pub-"),
    {ok, Pid} = emqtt:start_link([
        {host, mqtt_endpoint()}, {clientid, ClientID}, {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(Pid),
    {ok, Pid}.

mqtt_endpoint() ->
    case os:getenv("EMQX_MQTT_ENDPOINT") of
        false ->
            "127.0.0.1";
        Endpoint ->
            Endpoint
    end.

publish_data(Pid, SubClientID, Key) ->
    DataTopic = bin(["$ECQ/w/", SubClientID, "/", Key]),
    QoS = 1,
    Data = crypto:strong_rand_bytes(24),
    {ok, _} = emqtt:publish(Pid, DataTopic, Data, QoS),
    {ok, Data}.

start_subscriber(SubClientID) ->
    start_subscriber(SubClientID, []).

start_subscriber(SubClientID, Opts) ->
    Owner = self(),
    MsgHandler = #{publish => fun(Msg) -> Owner ! {publish_received, self(), SubClientID, Msg} end},
    Opts0 = [{clientid, SubClientID}, {proto_ver, v5}, {msg_handler, MsgHandler}],
    {ok, Pid} = emqtt:start_link(Opts0 ++ Opts),
    {ok, _} = emqtt:connect(Pid),
    QoS = 1,
    {ok, _, _} = emqtt:subscribe(Pid, sub_ecq_topic(SubClientID), QoS),
    {ok, _, _} = emqtt:subscribe(Pid, sub_normal_topic(SubClientID), QoS),
    {ok, Pid}.

sub_ecq_topic(SubClientID) ->
    bin(["$ECQ/", SubClientID, "/#"]).

sub_normal_topic(SubClientID) ->
    bin(["normal/", SubClientID, "/#"]).

stop_client(Pid) ->
    unlink(Pid),
    ok = emqtt:stop(Pid).

words(Topic) ->
    binary:split(Topic, <<"/">>, [global]).

random_clientid(Prefix) ->
    bin([
        Prefix,
        integer_to_list(erlang:system_time(nanosecond)),
        "-",
        integer_to_list(rand:uniform(1000000))
    ]).

bin(IoData) ->
    iolist_to_binary(IoData).

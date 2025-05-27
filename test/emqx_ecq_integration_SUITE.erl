%% This test suite verifies the plugin as a blackbox.
%% It assumes that the broker with the plugin installed
%% is running on endpoint defined in environment variable
%% EMQX_MQTT_ENDPOINT.
-module(emqx_ecq_integration_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

all() ->
    [
        F
     || {F, _} <- ?MODULE:module_info(exports),
        is_test_function(F)
    ].

is_test_function(F) ->
    case atom_to_list(F) of
        "t_" ++ _ -> true;
        _ -> false
    end.

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%% Consumer connect and subscribe, message is published while the consumer is online.
t_realtime_dispatch(_Config) ->
    SubClientID = random_clientid("sub-"),
    {ok, SubPid} = start_subscriber(SubClientID),
    try
        {ok, PubPid} = start_publisher(),
        try
            Key = <<"key/1">>,
            {ok, DataToExpect} = publish_data(PubPid, SubClientID, Key),
            ok = assert_payload_received(SubPid, Key, DataToExpect)
        after
            ok = stop_client(PubPid)
        end
    after
        ok = stop_client(SubPid)
    end.

%% A message is published by publisher, then a subscriber connects and subscribes.
t_late_subscribe(_Config) ->
    SubClientID = random_clientid("sub-"),
    Key = <<"key/1">>,
    {ok, PubPid} = start_publisher(),
    try
        {ok, Data} = publish_data(PubPid, SubClientID, Key),
        {ok, SubPid} = start_subscriber(SubClientID),
        try
            ok = assert_payload_received(SubPid, Key, Data)
        after
            ok = stop_client(SubPid)
        end
    after
        ok = stop_client(PubPid)
    end.

%% A subscriber disconnects, then reconnects with session persisted.
%% It will not queue the message in its session state while it is offline.
t_disconnected_session_does_not_queue_messages(_Config) ->
    %% TODO: start two subs, one nomral, one ECQ
    %% inspect the mqueue state of both subs
    ok.

t_compaction(_Config) ->
    %% TODO: publish the same key multiple times
    %% expect to consume the latest value
    ok.

assert_payload_received(SubPid, Key, Data) ->
    receive
        {publish_received, SubPid, SubClientID, Msg} ->
            ?assertMatch(#{qos := 1}, Msg),
            ?assertEqual(Data, maps:get(payload, Msg)),
            Topic = maps:get(topic, Msg),
            ExpectedTopic = bin(["$ECQ/", SubClientID, "/", Key]),
            ?assertEqual(ExpectedTopic, Topic),
            ok
    after 10000 ->
        ct:fail(#{reason => timeout})
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
    {ok, _, _} = emqtt:subscribe(Pid, sub_topic(SubClientID), QoS),
    {ok, Pid}.

sub_topic(SubClientID) ->
    bin(["$ECQ/", SubClientID, "/#"]).

stop_client(Pid) ->
    unlink(Pid),
    ok = emqtt:stop(Pid).

words(Topic) ->
    binary:split(Topic, <<"/">>, [global]).

random_clientid(Prefix) ->
    list_to_binary([Prefix, integer_to_list(erlang:system_time(millisecond))]).

bin(IoData) ->
    iolist_to_binary(IoData).

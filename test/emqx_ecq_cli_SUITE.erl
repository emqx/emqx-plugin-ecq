-module(emqx_ecq_cli_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include("emqx_ecq.hrl").

-compile(export_all).
-compile(nowarn_export_all).

%%--------------------------------------------------------------------
%% CT boilerplate
%%--------------------------------------------------------------------

all() -> emqx_ecq_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Token = login(),
    [{token, Token} | Config].

end_per_suite(Config) ->
    update_plugin_config(default_config(), Config),
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_gc_after_config_change(Config) ->
    {ok, PubPid} = start_publisher(),
    try
        ok = publish_batch(PubPid, 10),
        Status1 = get_status(),
        StatusRepl = get_status(3),
        ?assertMatch(#{<<"first">> := _}, maps:get(<<"messages">>, Status1)),
        %% data is not replicated to replicant nodes
        ?assertEqual(<<"empty">>, maps:get(<<"messages">>, StatusRepl)),
        PluginConfig = new_config(),
        timer:sleep(1000),
        ok = update_plugin_config(PluginConfig, Config),
        %% ensure ids and data are expired
        %% see new_config/0 for gc_interval and data_retention
        timer:sleep(1000),
        Status2 = get_status(),
        ?assertEqual(<<"empty">>, maps:get(<<"messages">>, Status2))
    after
        ok = stop_client(PubPid)
    end.

t_inspect(_Config) ->
    N = 10,
    {ok, PubPid} = start_publisher(),
    UniqueID = random_clientid("sub-"),
    Inspect1 = inspect(UniqueID),
    ?assertMatch(
        #{
            <<"queue">> := <<"empty">>,
            <<"acked">> := <<"none">>,
            <<"last_ack_ts">> := <<"none">>
        },
        Inspect1
    ),
    try
        ok = publish_batch(PubPid, N, UniqueID),
        Inspect2 = inspect(UniqueID),
        ?assertMatch(
            #{
                <<"queue">> := <<"[1,...,10]">>,
                <<"acked">> := <<"none">>,
                <<"last_ack_ts">> := <<"none">>
            },
            Inspect2
        ),
        {ok, SubPid} = start_subscriber(UniqueID),
        try
            _ = collect_messages(SubPid, N),
            Inspect3 = inspect(UniqueID),
            ?assertMatch(
                #{
                    <<"queue">> := <<"[1,...,10]">>,
                    <<"acked">> := 10,
                    <<"last_ack_ts">> := _
                },
                Inspect3
            )
        after
            ok = stop_client(SubPid)
        end
    after
        ok = stop_client(PubPid)
    end.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

start_publisher() ->
    {ok, Pid} = emqtt:start_link([
        {host, mqtt_endpoint()}, {clientid, <<"ecq-publisher">>}, {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(Pid),
    {ok, Pid}.

publish_batch(Pid, Count) ->
    publish_batch(Pid, Count, <<"sub1">>).

publish_batch(Pid, Count, SubClientID) ->
    Data = crypto:strong_rand_bytes(1024),
    lists:foreach(
        fun(I) ->
            Topic = bin(["$ECQ/w/", SubClientID, "/key", integer_to_binary(I)]),
            {ok, _} = emqtt:publish(Pid, Topic, Data, 1)
        end,
        lists:seq(1, Count)
    ).

stop_client(Pid) ->
    unlink(Pid),
    ok = emqtt:stop(Pid).

vin(UniqueId, I) ->
    list_to_binary(["vin-", integer_to_list(UniqueId), "-", integer_to_list(I)]).

mqtt_endpoint() ->
    case os:getenv("EMQX_MQTT_ENDPOINT") of
        false ->
            "127.0.0.1";
        Endpoint ->
            Endpoint
    end.

bin(S) ->
    iolist_to_binary(S).

url(Path) ->
    "http://" ++ mqtt_endpoint() ++ ":18083/api/v5/" ++ Path.

login() ->
    URL = url("login"),
    Headers = [{"Content-Type", "application/json"}],
    Body = json:encode(#{username => "admin", password => "public"}),
    {ok, {{_, 200, _}, _Headers, RespBody}} = httpc:request(
        post, {URL, Headers, "application/json", Body}, [], []
    ),
    maps:get(<<"token">>, json:decode(list_to_binary(RespBody))).

default_config() ->
    #{
        writer_pool_size => 0,
        gc_interval => <<"1h">>,
        data_retention => <<"7d">>,
        reader_batch_size => 5,
        write_timeout => <<"5s">>,
        read_timeout => <<"5s">>
    }.

new_config() ->
    maps:merge(default_config(), #{
        gc_interval => <<"1s">>,
        data_retention => <<"1s">>
    }).

name_vsn() ->
    "emqx_ecq-" ++ ?PLUGIN_VSN.

update_plugin_config(PluginConfig, CtConfig) ->
    URL = url("plugins/" ++ name_vsn() ++ "/config"),
    {token, Token} = lists:keyfind(token, 1, CtConfig),
    Headers = [
        {"Authorization", "Bearer " ++ binary_to_list(Token)},
        {"Content-Type", "application/json"}
    ],
    Body = json:encode(PluginConfig),
    {ok, {{_, StatusCode, _}, _, _}} = httpc:request(
        put, {URL, Headers, "application/json", Body}, [], []
    ),
    case StatusCode of
        200 -> ok;
        204 -> ok;
        _ -> {error, StatusCode}
    end.

get_status() ->
    get_status(1).

inspect(ClientID) ->
    Out = os:cmd("../../../../scripts/cli 1 inspect " ++ binary_to_list(ClientID)),
    ct:pal("Inspect: ~s", [Out]),
    json:decode(list_to_binary(Out)).

get_status(NodeId) ->
    Out = os:cmd("../../../../scripts/cli " ++ integer_to_list(NodeId) ++ " status"),
    ct:pal("Status: ~s", [Out]),
    json:decode(list_to_binary(Out)).

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

collect_messages(Pid, N) ->
    collect_messages(Pid, N, []).

collect_messages(_Pid, 0, Acc) ->
    {ok, lists:reverse(Acc)};
collect_messages(Pid, N, Acc) ->
    receive
        {publish_received, Pid, _SubClientID, Msg} ->
            collect_messages(Pid, N - 1, [Msg | Acc])
    after 10000 ->
        error(timeout)
    end.

random_clientid(Prefix) ->
    list_to_binary([Prefix, integer_to_list(erlang:system_time(millisecond))]).

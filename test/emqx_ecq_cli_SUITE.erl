-module(emqx_ecq_cli_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include("emqx_ecq.hrl").

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
    Token = login(),
    [{token, Token} | Config].

end_per_suite(Config) ->
    update_plugin_config(default_config(), Config),
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

t_gc_after_config_change(Config) ->
    {ok, PubPid} = start_publisher(),
    try
        ok = publish_batch(PubPid, 10),
        Status1 = get_status(),
        StatusRepl = get_status(3),
        ?assert(maps:get(<<"total_messages">>, Status1) >= 10),
        %% data is not replicated to replicant nodes
        ?assertEqual(0, maps:get(<<"total_messages">>, StatusRepl)),
        PluginConfig = new_config(),
        ok = update_plugin_config(PluginConfig, Config),
        %% ensure ids and data are expired
        %% see new_config/0 for gc_interval and data_retention
        timer:sleep(1000),
        Status2 = get_status(),
        ?assertEqual(0, maps:get(<<"total_messages">>, Status2))
    after
        ok = stop_client(PubPid)
    end.

start_publisher() ->
    {ok, Pid} = emqtt:start_link([
        {host, mqtt_endpoint()}, {clientid, <<"ecq-publisher">>}, {proto_ver, v5}
    ]),
    {ok, _} = emqtt:connect(Pid),
    {ok, Pid}.

publish_batch(Pid, Count) ->
    Data = crypto:strong_rand_bytes(1024),
    lists:foreach(
        fun(I) ->
            Topic = bin(["$ECQ/w/sub1/key", integer_to_binary(I)]),
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
    Body = emqx_utils_json:encode(#{username => "admin", password => "public"}),
    {ok, {{_, 200, _}, _Headers, RespBody}} = httpc:request(
        post, {URL, Headers, "application/json", Body}, [], []
    ),
    maps:get(<<"token">>, emqx_utils_json:decode(RespBody, [return_maps])).

default_config() ->
    #{
        writer_pool_size => 0,
        gc_interval => <<"1h">>,
        data_retention => <<"7d">>,
        reader_batch_size => 5
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
    Body = emqx_utils_json:encode(PluginConfig),
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

get_status(NodeId) ->
    Out = os:cmd("../../../../scripts/cli.sh " ++ integer_to_list(NodeId) ++ " status"),
    ct:pal("Status: ~s", [Out]),
    emqx_utils_json:decode(Out, [return_maps]).

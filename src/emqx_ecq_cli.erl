%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ecq_cli).

-export([cmd/1]).

-include("emqx_ecq.hrl").

cmd(["show-config" | More]) ->
    emqx_ctl:print("~ts", [format_config(More)]);
cmd(["gc"]) ->
    emqx_ecq_gc:run();
cmd(["status"]) ->
    show_status();
cmd(["inspect", ClientID]) ->
    inspect(ClientID);
cmd(_) ->
    emqx_ctl:usage(usages()).

format_config([]) ->
    format_config(["origin"]);
format_config(["origin" | MaybeJSON]) ->
    Config = emqx_plugin_helper:get_config(?PLUGIN_NAME_VSN),
    case MaybeJSON of
        ["--json" | _] ->
            [emqx_utils_json:encode(Config), "\n"];
        _ ->
            hocon_pp:do(Config, #{})
    end;
format_config(["inuse" | MaybeJSON]) ->
    Config = emqx_ecq_config:get(),
    case MaybeJSON of
        ["--json" | _] ->
            [emqx_utils_json:encode(Config), "\n"];
        _ ->
            io_lib:format("~p~n", [Config])
    end;
format_config(Args) ->
    emqx_ctl:print("bad args for show-config: ~p~n", [Args]),
    emqx_ctl:usage([usage(show_config)]).

usages() ->
    [
        usage(show_config),
        usage(gc),
        usage(status),
        usage(inspect)
    ].

show_status() ->
    emqx_ctl:print("~s~n", [emqx_utils_json:encode(get_status())]).

inspect(ClientID) ->
    emqx_ctl:print("~s~n", [emqx_utils_json:encode(emqx_ecq_store:inspect(bin(ClientID)))]).

bin(IoList) ->
    iolist_to_binary(IoList).

get_status() ->
    emqx_ecq_store:status().

usage(show_config) ->
    {"ecq show-config [origin|inuse] [--json]",
        "Show current config, 'origin' for original config,\n"
        "'inuse' for in-use (parsed) config, add '--json' for JSON format."};
usage(gc) ->
    {"ecq gc",
        "Run garbage collection on local node immediately.\n"
        "This command takes no effect on replicant nodes."};
usage(status) ->
    {"ecq status", "Show the status of the queues."};
usage(inspect) ->
    {"ecq inspect <clientid>", "Inspect the state of a consumer."}.

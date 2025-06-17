%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ecq_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([
    start/2,
    stop/1
]).

-export([
    on_config_changed/2,
    on_health_check/1
]).

-include("emqx_ecq.hrl").

start(_StartType, _StartArgs) ->
    MyRole = mria_config:whoami(),
    _ = persistent_term:put(?ROLE_PT_KEY, MyRole),
    ok = init_store(),
    {ok, Sup} = emqx_ecq_sup:start_link(MyRole),
    emqx_ecq:hook(),
    emqx_ctl:register_command(ecq, {emqx_ecq_cli, cmd}),
    {ok, Sup}.

stop(_State) ->
    emqx_ctl:unregister_command(ecq),
    emqx_ecq:unhook().

on_config_changed(OldConfig, NewConfig) ->
    emqx_ecq:on_config_changed(OldConfig, NewConfig).

on_health_check(Options) ->
    emqx_ecq:on_health_check(Options).

init_store() ->
    ok = emqx_ecq_store:open_db().

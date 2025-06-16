%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ecq_sup).

-behaviour(supervisor).

-export([start_link/1]).

-export([init/1]).

-include("emqx_ecq.hrl").

start_link(MyRole) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [MyRole]).

init([MyRole]) ->
    ok = emqx_ecq_inflight:create_tables(),
    ok = emqx_ecq_reader_reg:create_tables(),
    SupFlags = #{
        strategy => one_for_all,
        intensity => 100,
        period => 10
    },
    ConfigChildSpec = #{
        id => emqx_ecq,
        start => {emqx_ecq, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [emqx_ecq]
    },
    ClusterWatchChildSpec = #{
        id => emqx_ecq_writer_dist,
        start => {emqx_ecq_writer_dist, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [emqx_ecq_writer_dist]
    },
    WriterPoolSupSpec = writer_pool_sup_spec(),
    ReaderRegPoolSupSpec = reader_reg_table_sup_spec(),
    CommonChildren = [
        ConfigChildSpec,
        ClusterWatchChildSpec,
        ReaderRegPoolSupSpec
    ],
    Children =
        case MyRole of
            core ->
                %% Start writer pool and gc process on core nodes
                CommonChildren ++ [WriterPoolSupSpec];
            _ ->
                CommonChildren
        end,
    {ok, {SupFlags, Children}}.

resolve_pool_size() ->
    %% Get config from emqx_plugin_helper, but not from
    %% emqx_ecq_config because it's not initialized yet
    Config = emqx_plugin_helper:get_config(?PLUGIN_NAME_VSN),
    ConfigedSize = maps:get(<<"writer_pool_size">>, Config),
    resolve_pool_size(ConfigedSize).

resolve_pool_size(0) ->
    erlang:system_info(schedulers) * 10;
resolve_pool_size(N) when is_integer(N) andalso N > 0 ->
    N.

writer_pool_sup_spec() ->
    PoolModule = ?WRITER_POOL,
    PoolType = hash,
    PoolSize = resolve_pool_size(),
    MFA = {PoolModule, start_link, []},
    Pool = PoolModule,
    SupArgs = [Pool, PoolType, PoolSize, MFA],
    emqx_pool_sup:spec(emqx_ecq_writer_sup, SupArgs).

reader_reg_table_sup_spec() ->
    PoolModule = ?READER_REG_POOL,
    PoolType = hash,
    PoolSize = erlang:system_info(schedulers),
    MFA = {PoolModule, start_link, []},
    Pool = PoolModule,
    SupArgs = [Pool, PoolType, PoolSize, MFA],
    emqx_pool_sup:spec(emqx_ecq_reader_reg_sup, SupArgs).

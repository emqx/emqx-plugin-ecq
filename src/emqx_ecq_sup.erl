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

init([_MyRole]) ->
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
    ReaderRegPoolSupSpec = reader_reg_table_sup_spec(),
    MetricsWorkerSpec = emqx_ecq_metrics:spec(),
    Children = [
        MetricsWorkerSpec,
        ConfigChildSpec,
        ReaderRegPoolSupSpec
    ],
    {ok, {SupFlags, Children}}.

reader_reg_table_sup_spec() ->
    PoolModule = ?READER_REG_POOL,
    PoolType = hash,
    PoolSize = erlang:system_info(schedulers),
    MFA = {PoolModule, start_link, []},
    Pool = PoolModule,
    SupArgs = [Pool, PoolType, PoolSize, MFA],
    emqx_pool_sup:spec(emqx_ecq_reader_reg_sup, SupArgs).

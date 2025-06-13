%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Distribute load to one of core nodes.
-module(emqx_ecq_writer_dist).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    pick_core_node/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).
-define(PT_CACHE_KEY, {?MODULE, rpc_candidates}).
-define(TICK_INTERVAL, 10_000).

-include("emqx_ecq.hrl").

%% @doc Starts the load balancer for replicant nodes.
%% The core nodes will always write locally.
%% This means race condition between the core node writers,
%% The risk is low as we do not expect concurrent writes for the same clientid.
-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),
    ok = ekka:monitor(membership),
    ok = update_rpc_candidates(),
    erlang:start_timer(?TICK_INTERVAL, self(), tick),
    {ok, #{}}.

handle_call(Req, _From, State) ->
    ?LOG(error, "unexpected_call", #{server => ?MODULE, call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "unexpected_cast", #{server => ?MODULE, cast => Msg}),
    {noreply, State}.

handle_info({timeout, _, tick}, #{} = State) ->
    erlang:start_timer(?TICK_INTERVAL, self(), tick),
    ok = update_rpc_candidates(),
    {noreply, State};
handle_info({membership, _}, #{} = State) ->
    ok = update_rpc_candidates(),
    {noreply, State};
handle_info(Info, State) ->
    ?LOG(error, "unexpected_info", #{server => ?MODULE, info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ekka:unmonitor(membership).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

update_rpc_candidates() ->
    Cached = persistent_term:get(?PT_CACHE_KEY, #{}),
    NewMap = build_rpc_candidates(emqx_ecq_config:my_role()),
    case Cached =:= NewMap of
        true ->
            ok;
        false ->
            persistent_term:put(?PT_CACHE_KEY, NewMap)
    end,
    ok.

build_rpc_candidates(core) ->
    local;
build_rpc_candidates(_) ->
    Nodes = lists:sort(mria_membership:running_core_nodelist()),
    maps:from_list(lists:enumerate(Nodes)).

%% @doc Pick a core node for the given client ID.
pick_core_node(ClientId) ->
    case persistent_term:get(?PT_CACHE_KEY) of
        local ->
            {ok, node()};
        Nodes ->
            case maps:size(Nodes) > 0 of
                true ->
                    Key = erlang:phash2(ClientId, maps:size(Nodes)) + 1,
                    {ok, maps:get(Key, Nodes)};
                false ->
                    {error, no_running_core_nodes}
            end
    end.

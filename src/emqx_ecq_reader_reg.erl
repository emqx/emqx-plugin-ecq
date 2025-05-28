%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Reader process registry.
-module(emqx_ecq_reader_reg).

-behaviour(gen_server).

-export([start_link/2, create_tables/0]).

-export([add/2, lookup/1]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    handle_continue/2,
    terminate/2,
    code_change/3
]).

-include("emqx_ecq.hrl").

-define(REG_TAB, ecq_reader_reg).

%% @doc Create the tables. Called by supervisor.
create_tables() ->
    _ = ets:new(?REG_TAB, [
        named_table, set, public, {read_concurrency, true}, {write_concurrency, true}
    ]),
    ok.

%% @doc Start one writer process.
start_link(Pool, Id) ->
    gen_server:start_link(
        {local, emqx_utils:proc_name(Pool, Id)},
        ?MODULE,
        [Pool, Id],
        [{hibernate_after, 1000}]
    ).

%% @doc Start one reader process.
add(SubPid, ReaderPid) ->
    WriterPid = gproc_pool:pick_worker(?READER_REG_POOL, SubPid),
    gen_server:call(WriterPid, {add, SubPid, ReaderPid}, infinity).

%% @doc Lookup the reader process for the given subscriber process.
lookup(SubPid) ->
    case ets:lookup(?REG_TAB, SubPid) of
        [] ->
            [];
        [{_, ReaderPid}] ->
            [ReaderPid]
    end.

init([Pool, Id]) ->
    process_flag(trap_exit, true),
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #{pool => Pool, id => Id}}.

handle_call({add, SubPid, ReaderPid}, _From, State) ->
    _ = erlang:monitor(process, SubPid),
    ets:insert(?REG_TAB, {SubPid, ReaderPid}),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _Ref, process, Pid, _Reason}, State) ->
    emqx_ecq_inflight:delete_sub(Pid),
    ets:delete(?REG_TAB, Pid),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

handle_continue(_Continue, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

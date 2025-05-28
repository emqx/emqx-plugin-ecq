%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ecq_inflight).

-export([
    create_tables/0,
    exists/1,
    insert/1,
    delete/2,
    delete_sub/1
]).

-include("emqx_ecq.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-define(INFLIGHT(SubPid, Seqno), {SubPid, Seqno}).

create_tables() ->
    _ = ets:new(?INFLIGHT_TAB, [
        named_table, bag, public, {read_concurrency, true}, {write_concurrency, true}
    ]),
    ok.

%% @doc Check if the clientid has any inflight messages.
-spec exists(SubPid :: pid()) -> boolean().
exists(SubPid) ->
    ets:member(?INFLIGHT_TAB, SubPid).

%% @doc Insert a message sequence number into the inflight table for tracking.
%% The inflight table keeps track of messages that have been delivered but not yet acknowledged.
-spec insert([{SubPid :: pid(), Seqno :: seqno()}]) -> ok.
insert(Batch) ->
    ets:insert(?INFLIGHT_TAB, Batch),
    ok.

%% @doc Delete a message sequence number from the inflight table.
-spec delete(SubPid :: pid(), Seqno :: seqno()) -> ok.
delete(SubPid, Seqno) ->
    ets:delete_object(?INFLIGHT_TAB, ?INFLIGHT(SubPid, Seqno)),
    ok.

%% @doc Delete all inflight messages for the given subscriber.
-spec delete_sub(SubPid :: pid()) -> ok.
delete_sub(SubPid) ->
    ets:delete(?INFLIGHT_TAB, SubPid),
    ok.

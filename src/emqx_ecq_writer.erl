%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ecq_writer).

-export([start_link/2]).

-export([
    append/4
]).

%% RPC handlers
-export([
    append_local/1,
    notify_reader_local/1
]).

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

-record(append_req, {
    clientid :: clientid(),
    msg_key :: binary(),
    payload :: binary(),
    msg_ts :: ts(),
    deadline :: non_neg_integer()
}).

%% @doc Start one writer process.
start_link(Pool, Id) ->
    gen_server:start_link(
        {local, emqx_utils:proc_name(Pool, Id)},
        ?MODULE,
        [Pool, Id],
        [{hibernate_after, 1000}]
    ).

%% @doc Append a message to a queue.
-spec append(clientid(), msg_key(), binary(), ts()) -> ok | {error, Reason :: term()}.
append(ClientID, MsgKey, Payload, MsgTs) ->
    Req = #append_req{clientid = ClientID, msg_key = MsgKey, payload = Payload, msg_ts = MsgTs},
    case emqx_ecq_writer_dist:pick_core_node(ClientID) of
        {ok, Node} ->
            try erpc:call(Node, ?MODULE, append_local, [Req], 5_000) of
                {ok, Seqno} ->
                    ?LOG(debug, "new_message_appended", #{subscriber => ClientID, seqno => Seqno}),
                    %% new message appended, notify the subscriber.
                    maybe_notify_reader(ClientID);
                {error, Reason} ->
                    {error, Reason}
            catch
                C:E ->
                    {error, #{exception => C, cause => E}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Handle RPC call to append a message to a queue.
-spec append_local(#append_req{}) -> {ok, seqno()} | {error, Reason :: term()}.
append_local(#append_req{clientid = ClientID} = Req) ->
    %% TODO: configurable timeout
    Timeout = 5000,
    Deadline = now_ts() + Timeout,
    WriterPid = gproc_pool:pick_worker(?WRITER_POOL, ClientID),
    try
        gen_server:call(
            WriterPid,
            Req#append_req{deadline = Deadline},
            Timeout + 100
        )
    catch
        exit:Reason ->
            {error, Reason}
    end.

init([Pool, Id]) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #{pool => Pool, id => Id}, {continue, wait_for_tables}}.

handle_continue(wait_for_tables, State) ->
    emqx_ecq_store:wait_for_tables(),
    {noreply, State}.

handle_call(#append_req{} = Req, _From, State) ->
    case now_ts() > Req#append_req.deadline of
        true ->
            %% timedout, the caller will timeout the gen_call
            {noreply, State};
        false ->
            {ok, Seqno, NewState} = handle_append(Req, State),
            {reply, {ok, Seqno}, NewState}
    end;
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

now_ts() ->
    erlang:system_time(millisecond).

handle_append(
    #append_req{
        clientid = ClientID,
        msg_key = MsgKey,
        payload = Payload,
        msg_ts = Ts
    },
    State
) ->
    {ok, Seqno} = emqx_ecq_store:append(ClientID, MsgKey, Payload, Ts),
    {ok, Seqno, State}.

%% Lookup session registry to find if the consumer client is online. If yes, send the notification to the connected node.
maybe_notify_reader(ClientID) ->
    case emqx_cm:lookup_channels(ClientID) of
        [] ->
            ?LOG(debug, "no_subscriber_online", #{subscriber => ClientID}),
            %% no consumer client is online
            %% this should be the majority of the cases
            %% when session is not persisted.
            ok;
        Pids ->
            %% We do not know which one is the active session (during takeover),
            %% so send a notification to all the nodes where the client has a session process.
            Nodes = lists:usort([node(Pid) || Pid <- Pids]),
            ?LOG(debug, "notify_reader_nodes", #{subscriber => ClientID, nodes => Nodes}),
            lists:foreach(
                fun(Node) ->
                    notify_reader_node(Node, ClientID)
                end,
                Nodes
            )
    end.

notify_reader_node(Node, ClientID) ->
    erpc:cast(Node, ?MODULE, notify_reader_local, [ClientID]).

%% @doc Notify local readers of new data.
notify_reader_local(ClientID) ->
    emqx_ecq_reader:notify(ClientID).

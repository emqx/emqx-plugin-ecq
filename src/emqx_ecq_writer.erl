%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ecq_writer).

-export([
    append/4
]).

%% RPC handlers
-export([
    notify_reader_local/1
]).

-include("emqx_ecq.hrl").

-spec append(clientid(), msg_key(), binary(), ts()) -> ok | {error, Reason :: term()}.
append(ClientID, MsgKey, Payload, Ts) ->
    {Time, Res} = timer:tc(emqx_ecq_store, append, [ClientID, MsgKey, Payload, Ts]),
    case Res of
        ok ->
            ?LOG(debug, "append_message_success", #{
                subscriber => ClientID, msg_key => MsgKey, time_us => Time
            }),
            ok = maybe_notify_reader(ClientID),
            ok;
        {error, Reason} ->
            ?LOG(error, "append_message_error", #{
                subscriber => ClientID, msg_key => MsgKey, reason => Reason, time_us => Time
            }),
            {error, Reason}
    end.

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

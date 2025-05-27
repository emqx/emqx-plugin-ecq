%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ecq).

%% for #message{} record
%% no need for this include if we call emqx_message:to_map/1 to convert it to a map
-include_lib("emqx_plugin_helper/include/emqx.hrl").

%% for hook priority constants
-include_lib("emqx_plugin_helper/include/emqx_hooks.hrl").

%% for logging
-include_lib("emqx_plugin_helper/include/logger.hrl").

-include("emqx_ecq.hrl").

-export([
    hook/0,
    unhook/0,
    start_link/0
]).

-export([
    on_config_changed/2,
    on_health_check/1
]).

%% Hook callbacks
-export([
    on_message_publish/1,
    on_delivery_completed/2,
    on_session_subscribed/3,
    on_authorize/4
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(ECQ_SUB_TOPIC_PREFIX, "$ECQ").
-define(ECQ_PUB_TOPIC_PREFIX, "$ECQ/w").
-define(PROP_NAME_SEQNO, <<"ecq-seqno">>).

%% @doc
%% Called when the plugin application start
hook() ->
    %% Handle message publishes to ECQ topics.
    %% Hook it with higher priority than retainer
    %% which is also higher than rule engine
    %% so the message can be terminated here at this plugin,
    %% but not to leak to retainer or rule engine
    emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_RETAINER + 1),
    %% Handle PUBACK from subscribers (vehicles)
    emqx_hooks:add('delivery.completed', {?MODULE, on_delivery_completed, []}, ?HP_HIGHEST),
    %% Handle subscription to ECQ topic.
    emqx_hooks:add('session.subscribed', {?MODULE, on_session_subscribed, []}, ?HP_HIGHEST),
    %% Handle authorization to ECQ topic.
    %% Deny subscription to other clientids.
    emqx_hooks:add('client.authorize', {?MODULE, on_authorize, []}, ?HP_AUTHZ + 1),
    ok.

%% @doc
%% Called when the plugin stops
unhook() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    emqx_hooks:del('delivery.completed', {?MODULE, on_delivery_completed}),
    emqx_hooks:del('session.subscribed', {?MODULE, on_session_subscribed}),
    emqx_hooks:del('client.authorize', {?MODULE, on_authorize}),
    ok.

%%--------------------------------------------------------------------
%% Hook callbacks
%%--------------------------------------------------------------------

%% @doc
%% Handle message publishes to ECQ topics.
on_message_publish(
    #message{
        timestamp = Ts,
        topic = <<?ECQ_PUB_TOPIC_PREFIX, $/, ClientID_Key/binary>>,
        payload = Payload
    } = Message
) ->
    [ClientID, Key] = binary:split(ClientID_Key, <<"/">>),
    case emqx_ecq_writer:append(ClientID, Key, Payload, Ts) of
        ok ->
            %% log is done by the writer
            ok;
        {error, Reason} ->
            ?LOG(error, "failed_to_append_message", #{reason => Reason})
    end,
    {stop, do_not_allow_publish(Message)};
on_message_publish(Message) ->
    %% Other topics, non of our business, just pass it on.
    %% TODO: handle heartbeat
    {ok, Message}.

%% @doc
%% Called when PUBACK is received from the subscriber (vehicle).
on_delivery_completed(
    #message{topic = <<?ECQ_SUB_TOPIC_PREFIX, $/, _/binary>>} = Message, _
) ->
    case get_seqno(Message) of
        error ->
            ok;
        Seqno when is_integer(Seqno) ->
            ok = emqx_ecq_reader:acked(Seqno)
    end;
on_delivery_completed(_, _) ->
    %% Delivery completed is an indication of the client being online
    %% act as a heartbeat.
    ok = emqx_ecq_reader:heartbeat().

%% @doc
%% Handle subscription to ECQ topic.
on_session_subscribed(
    #{clientid := ClientID},
    <<?ECQ_SUB_TOPIC_PREFIX, $/, ClientID_Key/binary>>,
    _SubOpts
) ->
    case parse_clientid_key(ClientID_Key) of
        {ClientID0, <<"#">>} ->
            %% assert client id matches
            %% this should never happen as the on_authorize hook
            %% should have denied the subscription otherwise.
            ClientID =/= ClientID0 andalso error({client_id_mismatch, ClientID, ClientID0}),
            emqx_ecq_reader:subscribed(ClientID);
        _ ->
            %% Other topics, non of our business, just pass it on.
            ok
    end;
on_session_subscribed(_, _, _) ->
    %% Other topics, non of our business, just pass it on.
    ok.

on_authorize(
    #{clientid := ClientID},
    #{action_type := subscribe, qos := QoS},
    <<?ECQ_SUB_TOPIC_PREFIX, $/, Suffix/binary>>,
    _DefaultResult
) ->
    case binary:split(Suffix, <<"/">>) of
        [Owner, <<"#">>] when Owner =/= ClientID ->
            ?LOG(error, "not_owner_of_topic", #{owner => Owner}),
            {stop, #{result => deny, from => emqx_ecq}};
        [ClientID, <<"#">>] when QoS =/= 1 ->
            ?LOG(error, "ECQ_subscription_must_be_qos1", #{received => QoS}),
            {stop, #{result => deny, from => emqx_ecq}};
        _ ->
            ignore
    end;
on_authorize(_, _, _, _) ->
    ignore.

%%--------------------------------------------------------------------
%% Plugin callbacks
%%--------------------------------------------------------------------

%% @doc
%% - Return `{error, Error}' if the health check fails.
%% - Return `ok' if the health check passes.
on_health_check(_Options) ->
    ok.

%% @doc
%% - Return `{error, Error}' if the new config is invalid.
%% - Return `ok' if the config is valid and can be accepted.
on_config_changed(_OldConfig, NewConfig) ->
    Parsed = emqx_ecq_config:parse(NewConfig),
    Before = emqx_ecq_config:get_gc_interval(),
    emqx_ecq_config:put(Parsed),
    After = emqx_ecq_config:get_gc_interval(),
    case Before =:= After of
        true ->
            ok;
        false ->
            ?LOG(
                info,
                "gc_interval_changed_triggering_immediate_gc",
                #{old_interval => Before, new_interval => After}
            ),
            emqx_ecq_gc:run()
    end,
    ok = gen_server:cast(?MODULE, {on_changed, Parsed}).

%%--------------------------------------------------------------------
%% Working with config
%%--------------------------------------------------------------------

%% gen_server callbacks

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    erlang:process_flag(trap_exit, true),
    Config = emqx_plugin_helper:get_config(?PLUGIN_NAME_VSN),
    Parsed = emqx_ecq_config:parse(Config),
    emqx_ecq_config:put(Parsed),
    {ok, Config}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({on_changed, _ParsedConfig}, State) ->
    %% So far, no stateful operations neeeded for config changes
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Request, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

do_not_allow_publish(Message) ->
    Message#message{headers = #{allow_publish => false}}.

parse_clientid_key(ClientID_Key) ->
    [ClientID, Key] = binary:split(ClientID_Key, <<"/">>),
    {ClientID, Key}.

get_seqno(#message{headers = Headers}) ->
    try
        Props = maps:get(properties, Headers, #{}),
        UserProperties = maps:get('User-Property', Props),
        {_, Seqno0} = lists:keyfind(?PROP_NAME_SEQNO, 1, UserProperties),
        binary_to_integer(Seqno0)
    catch
        C:E:Stacktrace ->
            %% E.g. a message was delivered before the on_message_publish hook was added.
            %% but the on_delivery_completed hook was added while there was an inflight delivery.
            ?LOG(error, "no_seqno_for_delivered_message", #{
                exception => C,
                reason => E,
                stacktrace => Stacktrace
            }),
            error
    end.

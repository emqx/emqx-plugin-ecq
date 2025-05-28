%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ecq_gc).
-behaviour(gen_server).

-export([
    start_link/0,
    run/0
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

-include("emqx_ecq.hrl").

%% Get from persistent_term so we can inject values for testing.
%% by default, scan 100 clients per iteration, and delay 10ms between iterations.
-define(SCAN_LIMIT, persistent_term:get(emqx_ecq_gc_scan_limit, 100)).
-define(SCAN_DELAY, persistent_term:get(emqx_ecq_gc_scan_delay, 10)).
-define(GC, 'gc').
-define(GC(Last), {?GC, Last}).
-define(MIN_CLIENTID, <<>>).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Run garbage collection on local node immediately.
%% Then restart the timer with the current interval from config.
run() ->
    case whereis(?MODULE) of
        Pid when is_pid(Pid) ->
            erlang:send(Pid, ?GC(?MIN_CLIENTID));
        _ ->
            ok
    end,
    ok.

%% @private
init(_Args) ->
    process_flag(trap_exit, true),
    Interval = emqx_ecq_config:get_gc_interval(),
    InitialDelay = min(timer:minutes(30), Interval),
    TRef = schedule_gc(InitialDelay, ?MIN_CLIENTID),
    {ok, #{timer => TRef, last => ?MIN_CLIENTID, complete_runs => 0}}.

%% @private
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.
%% @private
handle_info(?GC(Last), #{timer := OldTRef, last := Last, complete_runs := CompleteRuns} = State) ->
    %% only run gc on the core nodes
    ?LOG(info, "gc_notification", #{last => Last}),
    _ = erlang:cancel_timer(OldTRef),
    NewState =
        case run_gc_index_and_payload(Last, ?SCAN_LIMIT) of
            complete ->
                Tref = schedule_gc(),
                State#{
                    timer := Tref,
                    last := ?MIN_CLIENTID,
                    complete_runs := CompleteRuns + 1
                };
            {continue, NewLast} ->
                Tref = schedule_gc(?SCAN_DELAY, NewLast),
                State#{timer := Tref, last := NewLast}
        end,
    {noreply, NewState};
handle_info(?GC(Last), State) ->
    ?LOG(warning, "ignored_gc_notification", #{last => Last, cause => running}),
    {noreply, State};
handle_info(Info, State) ->
    ?LOG(warning, "ignored_unknown_message", #{info => Info}),
    {noreply, State}.

%% @private
terminate(_Reason, #{timer := TRef}) ->
    _ = erlang:cancel_timer(TRef),
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
schedule_gc() ->
    Interval = emqx_ecq_config:get_gc_interval(),
    schedule_gc(Interval, ?GC(?MIN_CLIENTID)).

schedule_gc(Delay, ClientID) ->
    ?LOG(info, "garbage_collection_scheduled", #{delay => Delay, start_from => ClientID}),
    erlang:send_after(Delay, self(), ?GC(ClientID)).

%% @private Garbage collect the index and payload records for the given clientid.
%% TODO: garbage collect state records too (which should be controlled by a different config).
run_gc_index_and_payload(ClientID, 0) ->
    {continue, ClientID};
run_gc_index_and_payload(ClientID, Limit) ->
    Now = now_ts(),
    Retention = emqx_ecq_config:get_data_retention(),
    ExpireAt = Now - Retention,
    case emqx_ecq_store:gc(ClientID, ExpireAt) of
        complete ->
            complete;
        {continue, NewClientID} ->
            erlang:garbage_collect(self()),
            run_gc_index_and_payload(NewClientID, Limit - 1)
    end.

now_ts() ->
    erlang:system_time(millisecond).

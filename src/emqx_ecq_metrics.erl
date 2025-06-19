%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ecq_metrics).

-define(BUCKETS, [10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000]).
-define(METRICS_WORKER, ecq_metrics).

-export([
    spec/0,
    trans/3,
    get_metrics/1,
    observe_delivery_latency/2,
    observe/3,
    observe/5
]).

-export([
    format_hists/0,
    format_hists/1,
    format_hist/2,
    print_hists/0,
    print_hists/1,
    print_hist/2
]).

-define(FORMAT_WIDTH, 30).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

spec() ->
    emqx_metrics_worker:child_spec(
        %% Child ID
        ?MODULE,
        %% Name
        ?METRICS_WORKER,
        [
            {ds_latency, [
                {hist, append, ?BUCKETS},
                {hist, init_read_state, ?BUCKETS},
                {hist, persist_read_state, ?BUCKETS},
                {hist, read_batch, ?BUCKETS},
                {hist, get_streams, ?BUCKETS}
            ]},
            %% NOTE
            %% These metrics make sence only in benchmark tests, when all subscribers are online
            {delivery_latency, [
                {hist, forward, ?BUCKETS},
                {hist, ack, ?BUCKETS}
            ]}
        ]
    ).

trans(MetricName, TxOpts, TxFun) ->
    {Timer, Res} =
        try
            timer:tc(emqx_ds, trans, [TxOpts, TxFun])
        catch
            Class:Reason ->
                {error, {crash, {Class, Reason}}}
        end,
    ok = emqx_metrics_worker:observe_hist(?METRICS_WORKER, ds_latency, MetricName, Timer div 1000),
    Res.

observe(Id, Name, Fun) ->
    TimeBegin = erlang:monotonic_time(millisecond),
    try
        Fun()
    after
        TimeEnd = erlang:monotonic_time(millisecond),
        ok = emqx_metrics_worker:observe_hist(?METRICS_WORKER, Id, Name, TimeEnd - TimeBegin)
    end.

observe(Id, Name, M, F, A) ->
    observe(Id, Name, fun() -> erlang:apply(M, F, A) end).

observe_delivery_latency(Name, MessageTs) ->
    Latency = now_ts() - MessageTs,
    ok = emqx_metrics_worker:observe_hist(?METRICS_WORKER, delivery_latency, Name, Latency).

get_metrics(Id) ->
    emqx_metrics_worker:get_metrics(?METRICS_WORKER, Id).

format_hists() ->
    lists:map(fun format_hists/1, [ds_latency, delivery_latency]).

format_hists(Id) ->
    #{hists := Hists} = get_metrics(Id),
    lists:map(fun(HistName) -> format_hist(Id, HistName) end, maps:keys(Hists)).

format_hist(Id, HistName) ->
    #{hists := #{HistName := #{count := Count, bucket_counts := IncrementalBucketCounts}}} =
        ?MODULE:get_metrics(Id),
    do_format_hist(Id, HistName, Count, IncrementalBucketCounts).

print_hists() ->
    io:format(format_hists()).

print_hists(Id) ->
    io:format(format_hists(Id)).

print_hist(Id, HistName) ->
    io:format(format_hist(Id, HistName)).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

do_format_hist(Id, HistName, Count, IncrementalBucketCounts) ->
    BucketCounts = bucket_counts(IncrementalBucketCounts),
    {_, Counts} = lists:unzip(BucketCounts),
    MaxCount = lists:max(Counts),
    Rows0 = [format_row(BucketCount, MaxCount) || BucketCount <- BucketCounts],
    Rows1 = format_labels(Rows0),
    [
        io_lib:format("------~n~p.~p~n", [Id, HistName]),
        lists:map(
            fun([Label, Bar, RowCount]) ->
                io_lib:format("~s ~s~s~n", [Label, Bar, RowCount])
            end,
            Rows1
        ),
        io_lib:format("total: ~p~n", [Count])
    ].

%% Convert from incremental counts to individual bucket counts.
bucket_counts(IncrementalBucketCounts) -> do_bucket_counts([{0, 0} | IncrementalBucketCounts]).

do_bucket_counts([{_, PrevCount}, {Bucket, Count} | IncrementalBucketCounts]) ->
    [{Bucket, Count - PrevCount} | do_bucket_counts([{Bucket, Count} | IncrementalBucketCounts])];
do_bucket_counts(_) ->
    [].

format_row({Bucket, Count}, 0) ->
    [
        iolist_to_binary(io_lib:format("~p", [Bucket])),
        "|",
        iolist_to_binary(format_count(Count))
    ];
format_row({Bucket, Count}, MaxCount) ->
    Width = (?FORMAT_WIDTH * Count) div MaxCount,
    Bar = iolist_to_binary(["|", lists:duplicate(Width, $=)]),
    [iolist_to_binary(format_bucket(Bucket)), Bar, iolist_to_binary(format_count(Count))].

format_labels(Rows) ->
    LabelWidths = [size(Label) || [Label | _] <- Rows],
    MaxLabelWidth = lists:max(LabelWidths),
    [[pad_label(Label, MaxLabelWidth) | Rest] || [Label | Rest] <- Rows].

pad_label(Label, Width) ->
    PaddingWidth = Width - size(Label),
    Padding = lists:duplicate(PaddingWidth, " "),
    iolist_to_binary([Label, Padding]).

format_bucket(infinity) -> "inf";
format_bucket(Bucket) -> io_lib:format("~pms", [Bucket]).

format_count(0) -> "";
format_count(Count) -> io_lib:format(" ~p", [Count]).

now_ts() ->
    erlang:system_time(millisecond).

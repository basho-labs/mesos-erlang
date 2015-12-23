-module(erl_mesos_scheduler_SUITE).

-include_lib("common_test/include/ct.hrl").

-include_lib("erl_mesos.hrl").

-export([all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2]).

-export([bad_options/1,
         registered/1,
         disconnected/1,
         reregistered/1,
         resource_offers/1,
         offer_rescinded/1,
         error/1,
         accept/1]).

-record(state, {callback,
                test_pid}).

-define(LOG, false).

all() ->
    [bad_options, {group, mesos_cluster, [sequence]}].

groups() ->
    [{mesos_cluster, [registered,
                      disconnected,
                      reregistered,
                      resource_offers,
                      offer_rescinded,
                      error,
                      accept]}].

init_per_suite(Config) ->
    ok = erl_mesos:start(),
    Scheduler = erl_mesos_test_scheduler,
    SchedulerOptions = [{user, <<"user">>},
                        {name, <<"erl_mesos_test_scheduler">>}],
    {ok, Masters} = erl_mesos_cluster:config(masters, Config),
    MasterHosts = proplists:get_keys(Masters),
    Options = [{master_hosts, MasterHosts}],
    [{log, ?LOG},
     {scheduler, Scheduler},
     {scheduler_options, SchedulerOptions},
     {options, Options} |
     Config].

end_per_suite(_Config) ->
    application:stop(erl_mesos),
    ok.

init_per_group(mesos_cluster, Config) ->
    Config.

end_per_group(mesos_cluster, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    case lists:member(TestCase, proplists:get_value(mesos_cluster, groups())) of
        true ->
            stop_mesos_cluster(Config),
            start_mesos_cluster(Config),
            Config;
        false ->
            Config
    end.

end_per_testcase(TestCase, Config) ->
    case lists:member(TestCase, proplists:get_value(mesos_cluster, groups())) of
        true ->
            stop_mesos_cluster(Config),
            Config;
        false ->
            Config
    end.

%% Test functions.

%% Callbacks.

bad_options(Config) ->
    log("Bad options test cases", Config),
    Ref = {erl_mesos_scheduler, bad_options},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    %% Bad options.
    Options = undefined,
    {error, {bad_options, Options}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options, Config),
    %% Bad master hosts.
    MasterHosts = undefined,
    Options1 = [{master_hosts, MasterHosts}],
    {error, {bad_master_hosts, MasterHosts}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options1, Config),
    MasterHosts1 = [],
    Options2 = [{master_hosts, MasterHosts1}],
    {error, {bad_master_hosts, MasterHosts1}} =
        erl_mesos:start_scheduler(Ref, Scheduler, SchedulerOptions, Options2),
    %% Bad requset options.
    RequestOptions = undefined,
    Options3 = [{request_options, RequestOptions}],
    {error, {bad_request_options, RequestOptions}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options3, Config),
    %% Bad heartbeat timeout window.
    HeartbeatTimeoutWindow = undefined,
    Options4 = [{heartbeat_timeout_window, HeartbeatTimeoutWindow}],
    {error, {bad_heartbeat_timeout_window, HeartbeatTimeoutWindow}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options4, Config),
    %% Bad maximum number of resubscribe.
    MaxNumResubscribe = undefined,
    Options5 = [{max_num_resubscribe, MaxNumResubscribe}],
    {error, {bad_max_num_resubscribe, MaxNumResubscribe}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options5, Config),
    %% Bad resubscribe interval.
    ResubscribeInterval = undefined,
    Options6 = [{resubscribe_interval, ResubscribeInterval}],
    {error, {bad_resubscribe_interval, ResubscribeInterval}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options6, Config).

registered(Config) ->
    log("Registered test cases", Config),
    Ref = {erl_mesos_scheduler, registered},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    Capabilities = #framework_info_capabilitie{type =
                                               <<"REVOCABLE_RESOURCES">>},
    Labels = #labels{labels = [#label{key = <<"key">>, value = <<"value">>}]},
    SchedulerOptions1 = [{capabilities, Capabilities},
                         {labels, Labels} |
                         set_test_pid(SchedulerOptions)],
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options,
                              Config),
    {registered, SchedulerPid, SchedulerInfo, EventSubscribed} = recv_reply(),
    %% Test scheduler info.
    #scheduler_info{master_host = MasterHost,
                    subscribed = true,
                    framework_id = FrameworkId} = SchedulerInfo,
    MasterHosts = proplists:get_value(master_hosts, Options),
    true = lists:member(binary_to_list(MasterHost), MasterHosts),
    %% Test event subscribed.
    #event_subscribed{framework_id = FrameworkId,
                      heartbeat_interval_seconds = HeartbeatIntervalSeconds} =
        EventSubscribed,
    true = is_float(HeartbeatIntervalSeconds),
    %% Test scheduler state.
    FormatState = format_state(SchedulerPid),
    #state{callback = registered} = scheduler_state(FormatState),
    ok = stop_scheduler(Ref, Config).

disconnected(Config) ->
    log("Disconnected test cases", Config),
    Ref = {erl_mesos_scheduler, disconnected},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    Options1 = [{max_num_resubscribe, 0} | Options],
    %% Test connection crash.
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options1,
                              Config),
    {registered, SchedulerPid, _, _} = recv_reply(),
    FormatState = format_state(SchedulerPid),
    ClientRef = state_client_ref(FormatState),
    Pid = response_pid(ClientRef),
    exit(Pid, kill),
    {disconnected, SchedulerPid, SchedulerInfo} = recv_reply(),
    %% Test scheduler info.
    #scheduler_info{master_host = MasterHost,
                    subscribed = false} = SchedulerInfo,
    MasterHosts = proplists:get_value(master_hosts, Options),
    true = lists:member(binary_to_list(MasterHost), MasterHosts),
    %% Test scheduler state.
    {terminate, SchedulerPid, _, _, State} = recv_reply(),
    #state{callback = disconnected} = State,
    %% Test claster stop.
    Ref1 = {erl_mesos_scheduler, disconnected, 1},
    {ok, _} = start_scheduler(Ref1, Scheduler, SchedulerOptions1, Options1,
                              Config),
    {registered, SchedulerPid1, _, _} = recv_reply(),
    erl_mesos_cluster:stop(Config),
    {disconnected, SchedulerPid1, SchedulerInfo1} = recv_reply(),
    %% Test scheduler info.
    #scheduler_info{master_host = MasterHost1,
                    subscribed = false} = SchedulerInfo1,
    true = lists:member(binary_to_list(MasterHost1), MasterHosts),
    %% Test scheduler state.
    {terminate, SchedulerPid1, _, _, State1} = recv_reply(),
    #state{callback = disconnected} = State1.

reregistered(Config) ->
    log("Reregistered test cases", Config),
    Ref = {erl_mesos_scheduler, reregistered},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    Capabilities = #framework_info_capabilitie{type =
                                               <<"REVOCABLE_RESOURCES">>},
    Labels = #labels{labels = [#label{key = <<"key">>, value = <<"value">>}]},
    SchedulerOptions1 = [{capabilities, Capabilities},
                         {labels, Labels},
                         {failover_timeout, 1000} |
                         set_test_pid(SchedulerOptions)],
    Options = ?config(options, Config),
    Options1 = [{max_num_resubscribe, 2},
                {resubscribe_interval, 2000} |
                Options],
    %% Test connection crash.
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options1,
                              Config),
    {registered, SchedulerPid, _, _} = recv_reply(),
    FormatState = format_state(SchedulerPid),
    ClientRef = state_client_ref(FormatState),
    Pid = response_pid(ClientRef),
    exit(Pid, kill),
    {disconnected, SchedulerPid, _} = recv_reply(),
    {reregistered, SchedulerPid, SchedulerInfo} = recv_reply(),
    %% Test scheduler info.
    #scheduler_info{master_host = MasterHost,
                    subscribed = true} = SchedulerInfo,
    MasterHosts = proplists:get_value(master_hosts, Options),
    true = lists:member(binary_to_list(MasterHost), MasterHosts),
    %% Test scheduler state.
    FormatState1 = format_state(SchedulerPid),
    #state{callback = reregistered} = scheduler_state(FormatState1),
    ok = stop_scheduler(Ref, Config),
    Ref1 = {erl_mesos_scheduler, reregistered, 1},
    %% Test stop master.
    {ok, _} = start_scheduler(Ref1, Scheduler, SchedulerOptions1, Options1,
                              Config),
    {registered, SchedulerPid1, SchedulerInfo1, _} = recv_reply(),
    #scheduler_info{master_host = MasterHost1} = SchedulerInfo1,
    MasterContainer = master_container(MasterHost1, Config),
    stop_mesos_master(MasterContainer, Config),
    {disconnected, SchedulerPid1, _} = recv_reply(),
    {reregistered, SchedulerPid1, SchedulerInfo2} = recv_reply(),
    %% Test scheduler info.
    #scheduler_info{master_host = MasterHost2,
                    subscribed = true} = SchedulerInfo2,
    true = MasterHost2 =/= MasterHost1,
    true = lists:member(binary_to_list(MasterHost2), MasterHosts),
    %% Test scheduler state.
    FormatState2 = format_state(SchedulerPid1),
    #state{callback = reregistered} = scheduler_state(FormatState2),
    ok = stop_scheduler(Ref1, Config).

resource_offers(Config) ->
    log("Resource offers test cases", Config),
    Ref = {erl_mesos_scheduler, resource_offers},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options,
                              Config),
    {registered, SchedulerPid, _, _} = recv_reply(),
    stop_mesos_slave(Config),
    start_mesos_slave(Config),
    {resource_offers, SchedulerPid, SchedulerInfo, EventOffers} = recv_reply(),
    %% Test scheduler info.
    #scheduler_info{subscribed = true} = SchedulerInfo,
    %% Test event offer.
    #event_offers{offers = Offers} = EventOffers,
    [Offer | _] = Offers,
    #offer{id = Id,
           framework_id = FrameworkId,
           agent_id = AgentId,
           hostname = Hostname,
           url = Url,
           resources = Resources,
           attributes = undefined,
           executor_ids = undefined,
           unavailability = undefined} = Offer,
    #offer_id{value = OfferIdValue} = Id,
    true = is_binary(OfferIdValue),
    #framework_id{value = FrameworkIdValue} = FrameworkId,
    true = is_binary(FrameworkIdValue),
    #agent_id{value = AgentIdValue} = AgentId,
    true = is_binary(AgentIdValue),
    true = is_binary(Hostname),
    true = is_record(Url, url),
    ResourceFun = fun(#resource{name = Name,
                                type = Type,
                                scalar = Scalar,
                                ranges = Ranges}) ->
                        true = is_binary(Name),
                        true = is_binary(Type),
                        case Type of
                            <<"SCALAR">> ->
                                #value_scalar{value = ScalarValue} = Scalar,
                                true = is_float(ScalarValue),
                                undefined = Ranges;
                            <<"RANGES">> ->
                                undefined = Scalar,
                                #value_ranges{range = ValueRanges} = Ranges,
                                [ValueRange | _] = ValueRanges,
                                #value_range{'begin' = ValueRangeBegin,
                                             'end' = ValueRangeEnd} =
                                    ValueRange,
                                true = is_integer(ValueRangeBegin),
                                true = is_integer(ValueRangeEnd)
                        end
                  end,
    lists:map(ResourceFun, Resources),
    %% Test scheduler state.
    FormatState = format_state(SchedulerPid),
    #state{callback = resource_offers} = scheduler_state(FormatState),
    stop_mesos_slave(Config),
    ok = stop_scheduler(Ref, Config).

offer_rescinded(Config) ->
    log("Offer rescinded test cases", Config),
    Ref = {erl_mesos_scheduler, offer_rescinded},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options,
                              Config),
    {registered, SchedulerPid, _, _} = recv_reply(),
    stop_mesos_slave(Config),
    start_mesos_slave(Config),
    {resource_offers, SchedulerPid, _SchedulerInfo, _EventOffers} =
        recv_reply(),
    %% Test scheduler info.
    stop_mesos_slave(Config),
    {offer_rescinded, SchedulerPid, SchedulerInfo, EventRescind} = recv_reply(),
    #scheduler_info{subscribed = true} = SchedulerInfo,
    %% Test event rescind.
    #event_rescind{offer_id = OfferId} = EventRescind,
    #offer_id{value = Value} = OfferId,
    true = is_binary(Value),
    %% Test scheduler state.
    FormatState = format_state(SchedulerPid),
    #state{callback = offer_rescinded} = scheduler_state(FormatState),
    ok = stop_scheduler(Ref, Config).

error(Config) ->
    log("Error test cases test cases", Config),
    Ref = {erl_mesos_scheduler, error},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = [{failover_timeout, 1} |
                         set_test_pid(SchedulerOptions)],
    Options = ?config(options, Config),
    Options1 = [{max_num_resubscribe, 1},
                {resubscribe_interval, 1500} |
                Options],
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options1,
                              Config),
    {registered, SchedulerPid, _, _} = recv_reply(),
    FormatState = format_state(SchedulerPid),
    ClientRef = state_client_ref(FormatState),
    Pid = response_pid(ClientRef),
    exit(Pid, kill),
    {disconnected, SchedulerPid, _} = recv_reply(),
    {error, SchedulerPid, _SchedulerInfo, EventError} = recv_reply(),
    %% Test error event.
    #event_error{message = Message} = EventError,
    true = is_binary(Message),
    %% Test scheduler state.
    {terminate, SchedulerPid, _, _, State} = recv_reply(),
    #state{callback = error} = State.

%% Calls.

accept(Config) ->
    log("Accept test cases test cases", Config),
    Ref = {erl_mesos_scheduler, accept},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options,
                              Config),
    {registered, SchedulerPid, _, _} = recv_reply(),
    stop_mesos_slave(Config),
    start_mesos_slave(Config),
    {resource_offers, SchedulerPid, _SchedulerInfo, EventOffers} =
        recv_reply(),
    #event_offers{offers = [Offer | _]} = EventOffers,
    #offer{id = OfferId, agent_id = AgentId} = Offer,
    TaskId = timestamp_task_id(),
    SchedulerPid ! {accept, OfferId, AgentId, TaskId},
    {accept, ok} = recv_reply(),
    stop_mesos_slave(Config),
    ok = stop_scheduler(Ref, Config).

%% Internal functions.

start_mesos_cluster(Config) ->
    StartRes = erl_mesos_cluster:start(Config),
    log("Start test mesos cluster.~n"
        "Output: ~s~n",
        [StartRes],
        Config),
    {ok, StartTimeout} = erl_mesos_cluster:config(start_timeout, Config),
    timer:sleep(StartTimeout).

stop_mesos_cluster(Config) ->
    StopRes = erl_mesos_cluster:stop(Config),
    log("Stop test mesos cluster.~n"
        "Output: ~s~n",
        [StopRes],
        Config).

stop_mesos_master(MasterContainer, Config) ->
    StopRes = erl_mesos_cluster:stop_master(MasterContainer, Config),
    log("Stop test mesos master.~n"
        "Master container: ~s~n"
        "Output: ~s~n",
        [MasterContainer, StopRes],
        Config),
    {ok, LeaderElectionTimeout} =
        erl_mesos_cluster:config(leader_election_timeout, Config),
    timer:sleep(LeaderElectionTimeout).

master_container(MasterHost, Config) ->
    {ok, Masters} = erl_mesos_cluster:config(masters, Config),
    proplists:get_value(binary_to_list(MasterHost), Masters).

start_mesos_slave(Config) ->
    StartRes = erl_mesos_cluster:start_slave(Config),
    log("Start test mesos slave.~n"
        "Output: ~s~n",
        [StartRes],
        Config),
    {ok, SlaveStartTimeout} = erl_mesos_cluster:config(slave_start_timeout,
                                                       Config),
    timer:sleep(SlaveStartTimeout).

stop_mesos_slave(Config) ->
    StopRes = erl_mesos_cluster:stop_slave(Config),
    log("Stop test mesos slave.~n"
        "Output: ~s~n",
        [StopRes],
        Config).

start_scheduler(Ref, Scheduler, SchedulerOptions, Options, Config) ->
    log("Start scheduler~n"
        "Ref == ~p~n"
        "Scheduler == ~p~n"
        "Scheduler options == ~p~n"
        "Options == ~p~n",
        [Ref, Scheduler, SchedulerOptions, Options],
        Config),
    erl_mesos:start_scheduler(Ref, Scheduler, SchedulerOptions, Options).

stop_scheduler(Ref, Config) ->
    log("Stop scheduler~n"
        "Ref == ~p~n",
        [Ref],
        Config),
    erl_mesos:stop_scheduler(Ref).

set_test_pid(SchedulerOptions) ->
    [{test_pid, self()} | SchedulerOptions].

format_state(SchedulerPid) ->
    {status, _Pid, _Module, Items} = sys:get_status(SchedulerPid),
    {data, Format} = lists:last(lists:last(Items)),
    proplists:get_value("State", Format).

scheduler_state(FormatState) ->
    proplists:get_value("Scheduler state", FormatState).

state_client_ref(FormatState) ->
    State = proplists:get_value("State", FormatState),
    proplists:get_value(client_ref, State).

response_pid(ClientRef) ->
    {ok, Pid} = hackney_manager:async_response_pid(ClientRef),
    Pid.

recv_reply() ->
    receive
        {registered, SchedulerPid, SchedulerInfo, EventSubscribed} ->
            {registered, SchedulerPid, SchedulerInfo, EventSubscribed};
        {disconnected, SchedulerPid, SchedulerInfo} ->
            {disconnected, SchedulerPid, SchedulerInfo};
        {reregistered, SchedulerPid, SchedulerInfo} ->
            {reregistered, SchedulerPid, SchedulerInfo};
        {resource_offers, SchedulerPid, SchedulerInfo, EventOffers} ->
            {resource_offers, SchedulerPid, SchedulerInfo, EventOffers};
        {offer_rescinded, SchedulerPid, SchedulerInfo, EventRescind} ->
            {offer_rescinded, SchedulerPid, SchedulerInfo, EventRescind};
        {status_update, SchedulerPid, SchedulerInfo, EventUpdate} ->
            {status_update, SchedulerPid, SchedulerInfo, EventUpdate};
        {slave_lost, SchedulerPid, SchedulerInfo, EventFailure} ->
            {slave_lost, SchedulerPid, SchedulerInfo, EventFailure};
        {executor_lost, SchedulerPid, SchedulerInfo, EventFailure} ->
            {executor_lost, SchedulerPid, SchedulerInfo, EventFailure};
        {error, SchedulerPid, SchedulerInfo, ErrorEvent} ->
            {error, SchedulerPid, SchedulerInfo, ErrorEvent};
        {accept, Accept} ->
            {accept, Accept};
        {terminate, SchedulerPid, SchedulerInfo, Reason, State} ->
            {terminate, SchedulerPid, SchedulerInfo, Reason, State};
        Reply ->
            {error, {bad_reply, Reply}}
    after 5000 ->
        {error, timeout}
    end.

timestamp_task_id() ->
    {MegaSecs, Secs, MicroSecs} = os:timestamp(),
    Timestamp = (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs,
    #task_id{value = list_to_binary(integer_to_list(Timestamp))}.


log(Format, Config) ->
    log(Format, [], Config).

log(Format, Data, Config) ->
    case ?config(log, Config) of
        true ->
            ct:pal(Format, Data);
        false ->
            ok
    end.

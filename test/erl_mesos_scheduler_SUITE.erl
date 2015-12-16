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
         error/1]).

-record(state, {callback,
                test_pid}).

all() ->
    [bad_options, {group, cluster}].

groups() ->
    [{cluster, [registered,
                disconnected,
                reregistered,
                resource_offers,
                offer_rescinded,
                error]}].

init_per_suite(Config) ->
    ok = erl_mesos:start(),
    Scheduler = erl_mesos_test_scheduler,
    SchedulerOptions = [{user, <<"user">>},
                        {name, <<"erl_mesos_test_scheduler">>}],
    {ok, Masters} = erl_mesos_cluster:config(masters, Config),
    MasterHosts = proplists:get_keys(Masters),
    Options = [{master_hosts, MasterHosts}],
    [{scheduler, Scheduler},
     {scheduler_options, SchedulerOptions},
     {options, Options} |
     Config].

end_per_suite(_Config) ->
    application:stop(erl_mesos),
    ok.

init_per_group(cluster, Config) ->
    Config.

end_per_group(cluster, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    case lists:member(TestCase, proplists:get_value(cluster, groups())) of
        true ->
            stop_mesos_cluster(Config),
            start_mesos_cluster(Config),
            mesos_cluster_start_timeout_sleep(Config),
            Config;
        false ->
            Config
    end.

end_per_testcase(TestCase, Config) ->
    case lists:member(TestCase, proplists:get_value(cluster, groups())) of
        true ->
            stop_mesos_cluster(Config),
            Config;
        false ->
            Config
    end.

%% Test functions.

bad_options(Config) ->
    ct:pal("** Bad options test cases"),
    Ref = {erl_mesos_scheduler, bad_options},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    %% Bad options.
    Options = undefined,
    {error, {bad_options, Options}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options),
    %% Bad master hosts.
    MasterHosts = undefined,
    Options1 = [{master_hosts, MasterHosts}],
    {error, {bad_master_hosts, MasterHosts}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options1),
    MasterHosts1 = [],
    Options2 = [{master_hosts, MasterHosts1}],
    {error, {bad_master_hosts, MasterHosts1}} =
        erl_mesos:start_scheduler(Ref, Scheduler, SchedulerOptions, Options2),
    %% Bad subscribe requset options.
    SubscribeReqOptions = undefined,
    Options3 = [{subscribe_req_options, SubscribeReqOptions}],
    {error, {bad_subscribe_req_options, SubscribeReqOptions}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options3),
    %% Bad heartbeat timeout window.
    HeartbeatTimeoutWindow = undefined,
    Options4 = [{heartbeat_timeout_window, HeartbeatTimeoutWindow}],
    {error, {bad_heartbeat_timeout_window, HeartbeatTimeoutWindow}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options4),
    %% Bad maximum number of resubscribe.
    MaxNumResubscribe = undefined,
    Options5 = [{max_num_resubscribe, MaxNumResubscribe}],
    {error, {bad_max_num_resubscribe, MaxNumResubscribe}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options5),
    %% Bad resubscribe interval.
    ResubscribeInterval = undefined,
    Options6 = [{resubscribe_interval, ResubscribeInterval}],
    {error, {bad_resubscribe_interval, ResubscribeInterval}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options6).

registered(Config) ->
    ct:pal("** Registered test cases"),
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
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options),
    {registered, SchedulerPid, SchedulerInfo, SubscribedEvent} = recv_reply(),
    %% Test scheduler info.
    #scheduler_info{master_host = MasterHost,
                    subscribed = true,
                    framework_id = FrameworkId} = SchedulerInfo,
    MasterHosts = proplists:get_value(master_hosts, Options),
    true = lists:member(binary_to_list(MasterHost), MasterHosts),
    %% Test subscribed event.
    #subscribed_event{framework_id = FrameworkId,
                      heartbeat_interval_seconds = HeartbeatIntervalSeconds} =
        SubscribedEvent,
    true = is_integer(HeartbeatIntervalSeconds),
    %% Test scheduler state.
    FormatState = format_state(SchedulerPid),
    #state{callback = registered} = scheduler_state(FormatState),
    ok = stop_scheduler(Ref).

disconnected(Config) ->
    ct:pal("** Disconnected test cases"),
    Ref = {erl_mesos_scheduler, disconnected},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    Options1 = [{max_num_resubscribe, 0} | Options],
    %% Test connection crash.
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options1),
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
    {ok, _} = start_scheduler(Ref1, Scheduler, SchedulerOptions1, Options1),
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
    ct:pal("** Reregistered test cases"),
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
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options1),
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
    ok = stop_scheduler(Ref),
    Ref1 = {erl_mesos_scheduler, reregistered, 1},
    %% Test stop master.
    {ok, _} = start_scheduler(Ref1, Scheduler, SchedulerOptions1, Options1),
    {registered, SchedulerPid1, SchedulerInfo1, _} = recv_reply(),
    #scheduler_info{master_host = MasterHost1} = SchedulerInfo1,
    MasterContainer = master_container(MasterHost1, Config),
    mesos_cluster_stop_master(Config, MasterContainer),
    mesos_cluster_leader_choose_timeout_sleep(Config),
    {disconnected, SchedulerPid1, _} = recv_reply(),
    {reregistered, SchedulerPid1, SchedulerInfo2} = recv_reply(),
    %% Test scheduler info.
    #scheduler_info{master_host = MasterHost2,
                    subscribed = true} = SchedulerInfo2,
    true = MasterHost2 =/= MasterHost1,
    true = lists:member(binary_to_list(MasterHost2), MasterHosts),
    %% Test scheduler state.
    FormatState2 = format_state(SchedulerPid1),
    #state{callback = reregistered} = scheduler_state(FormatState2).

resource_offers(Config) ->
    ct:pal("** Resource offers test cases"),
    Ref = {erl_mesos_scheduler, resource_offers},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options),
    {registered, SchedulerPid, _, _} = recv_reply(),
    mesos_cluster_start_slave(Config),
    timer:sleep(5000),
    {resource_offers, SchedulerPid, SchedulerInfo, OffersEvent} = recv_reply(),
    %% Test scheduler info.
    #scheduler_info{subscribed = true} = SchedulerInfo,
    %% Test offer event.
    #offers_event{offers = Offers} = OffersEvent,
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
    mesos_cluster_stop_slave(Config).

offer_rescinded(Config) ->
    ct:pal("** Offer rescinded test cases"),
    Ref = {erl_mesos_scheduler, offer_rescinded},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options),
    {registered, SchedulerPid, _, _} = recv_reply(),
    mesos_cluster_start_slave(Config),
    timer:sleep(5000),
    {resource_offers, SchedulerPid, _SchedulerInfo, _OffersEvent} =
        recv_reply(),
    %% Test scheduler info.
    mesos_cluster_stop_slave(Config),
    {offer_rescinded, SchedulerPid, SchedulerInfo, RescindEvent} = recv_reply(),
    #scheduler_info{subscribed = true} = SchedulerInfo,
    %% Test rescind event.
    #rescind_event{offer_id = OfferId} = RescindEvent,
    #offer_id{value = Value} = OfferId,
    true = is_binary(Value),
    %% Test scheduler state.
    FormatState = format_state(SchedulerPid),
    #state{callback = offer_rescinded} = scheduler_state(FormatState).

error(Config) ->
    ct:pal("** Error test cases test cases"),
    Ref = {erl_mesos_scheduler, error},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = [{failover_timeout, 1} |
                         set_test_pid(SchedulerOptions)],
    Options = ?config(options, Config),
    Options1 = [{max_num_resubscribe, 1},
                {resubscribe_interval, 1500} |
                Options],
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options1),
    {registered, SchedulerPid, _, _} = recv_reply(),
    FormatState = format_state(SchedulerPid),
    ClientRef = state_client_ref(FormatState),
    Pid = response_pid(ClientRef),
    exit(Pid, kill),
    {disconnected, SchedulerPid, _} = recv_reply(),
    {error, SchedulerPid, _SchedulerInfo, ErrorEvent} = recv_reply(),
    %% Test error event.
    #error_event{message = Message} = ErrorEvent,
    true = is_binary(Message),
    %% Test scheduler state.
    {terminate, SchedulerPid, _, _, State} = recv_reply(),
    #state{callback = error} = State.

%% Internal functions.

start_mesos_cluster(Config) ->
    StartRes = erl_mesos_cluster:start(Config),
    ct:pal("Start test mesos cluster.~n"
           "Output: ~s~n",
           [StartRes]),
    ok.

stop_mesos_cluster(Config) ->
    StopRes = erl_mesos_cluster:stop(Config),
    ct:pal("Stop test mesos cluster.~n"
           "Output: ~s~n",
           [StopRes]),
    ok.

mesos_cluster_stop_master(Config, MasterContainer) ->
    StopRes = erl_mesos_cluster:stop_master(Config, MasterContainer),
    ct:pal("Stop test mesos master.~n"
           "Master container: ~s~n"
           "Output: ~s~n",
           [MasterContainer, StopRes]),
    ok.

mesos_cluster_start_timeout_sleep(Config) ->
    {ok, StartTimeout} = erl_mesos_cluster:config(start_timeout, Config),
    timer:sleep(StartTimeout).

mesos_cluster_leader_choose_timeout_sleep(Config) ->
    {ok, LeaderChooseTimeout} = erl_mesos_cluster:config(leader_choose_timeout,
                                                         Config),
    timer:sleep(LeaderChooseTimeout).

mesos_cluster_start_slave(Config) ->
    erl_mesos_cluster:start_slave(Config).

mesos_cluster_stop_slave(Config) ->
    erl_mesos_cluster:stop_slave(Config).

set_test_pid(SchedulerOptions) ->
    [{test_pid, self()} | SchedulerOptions].

recv_reply() ->
    receive
        {registered, SchedulerPid, SchedulerInfo, SubscribedEvent} ->
            {registered, SchedulerPid, SchedulerInfo, SubscribedEvent};
        {disconnected, SchedulerPid, SchedulerInfo} ->
            {disconnected, SchedulerPid, SchedulerInfo};
        {reregistered, SchedulerPid, SchedulerInfo} ->
            {reregistered, SchedulerPid, SchedulerInfo};
        {resource_offers, SchedulerPid, SchedulerInfo, OffersEvent} ->
            {resource_offers, SchedulerPid, SchedulerInfo, OffersEvent};
        {offer_rescinded, SchedulerPid, SchedulerInfo, RescindEvent} ->
            {offer_rescinded, SchedulerPid, SchedulerInfo, RescindEvent};
        {error, SchedulerPid, SchedulerInfo, ErrorEvent} ->
            {error, SchedulerPid, SchedulerInfo, ErrorEvent};
        {terminate, SchedulerPid, SchedulerInfo, Reason, State} ->
            {terminate, SchedulerPid, SchedulerInfo, Reason, State};
        Reply ->
            {error, {bad_reply, Reply}}
    after 5000 ->
        {error, timeout}
    end.

scheduler_state(FormatState) ->
    proplists:get_value("Scheduler state", FormatState).

state_client_ref(FormatState) ->
    State = proplists:get_value("State", FormatState),
    proplists:get_value(client_ref, State).

format_state(SchedulerPid) ->
    {status, _Pid, _Module, Items} = sys:get_status(SchedulerPid),
    {data, Format} = lists:last(lists:last(Items)),
    proplists:get_value("State", Format).

start_scheduler(Ref, Scheduler, SchedulerOptions, Options) ->
    ct:pal("** Start scheduler~n"
           "** Ref == ~p~n"
           "** Scheduler == ~p~n"
           "** Scheduler options == ~p~n"
           "** Options == ~p~n",
           [Ref, Scheduler, SchedulerOptions, Options]),
    erl_mesos:start_scheduler(Ref, Scheduler, SchedulerOptions, Options).

stop_scheduler(Ref) ->
    ct:pal("** Stop scheduler~n"
           "** Ref == ~p~n",
           [Ref]),
    erl_mesos:stop_scheduler(Ref).

response_pid(ClientRef) ->
    {ok, Pid} = hackney_manager:async_response_pid(ClientRef),
    Pid.

master_container(MasterHost, Config) ->
    {ok, Masters} = erl_mesos_cluster:config(masters, Config),
    proplists:get_value(binary_to_list(MasterHost), Masters).

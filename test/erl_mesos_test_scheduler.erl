-module(erl_mesos_test_scheduler).

-behaviour(erl_mesos_scheduler).

-include_lib("erl_mesos.hrl").

-export([init/1,
         registered/3,
         reregistered/2,
         disconnected/2,
         resource_offers/3,
         offer_rescinded/3,
         status_update/3,
         framework_message/3,
         slave_lost/3,
         executor_lost/3,
         error/3,
         handle_info/3,
         terminate/3]).

-record(state, {callback,
                test_pid}).

%% erl_mesos_scheduler callback functions.

init(Options) ->
    FrameworkInfo = framework_info(Options),
    TestPid = proplists:get_value(test_pid, Options),
    {ok, FrameworkInfo, true, #state{callback = init,
                                     test_pid = TestPid}}.

registered(SchedulerInfo, EventSubscribed,
           #state{test_pid = TestPid} = State) ->
    reply(TestPid, {registered, self(), SchedulerInfo, EventSubscribed}),
    {ok, State#state{callback = registered}}.

disconnected(SchedulerInfo, #state{test_pid = TestPid} = State) ->
    reply(TestPid, {disconnected, self(), SchedulerInfo}),
    {ok, State#state{callback = disconnected}}.

reregistered(SchedulerInfo, #state{test_pid = TestPid} = State) ->
    reply(TestPid, {reregistered, self(), SchedulerInfo}),
    {ok, State#state{callback = reregistered}}.

resource_offers(SchedulerInfo, EventOffers,
                #state{test_pid = TestPid} = State) ->
    reply(TestPid, {resource_offers, self(), SchedulerInfo, EventOffers}),
    {ok, State#state{callback = resource_offers}}.

offer_rescinded(SchedulerInfo, EventRescind,
                #state{test_pid = TestPid} = State) ->
    reply(TestPid, {offer_rescinded, self(), SchedulerInfo, EventRescind}),
    {ok, State#state{callback = offer_rescinded}}.

status_update(SchedulerInfo, EventUpdate, #state{test_pid = TestPid} = State) ->
    reply(TestPid, {status_update, self(), SchedulerInfo, EventUpdate}),
    {ok, State#state{callback = status_update}}.

framework_message(SchedulerInfo, EventMessage,
                  #state{test_pid = TestPid} = State) ->
    reply(TestPid, {framework_message, self(), SchedulerInfo, EventMessage}),
    {ok, State#state{callback = framework_message}}.

slave_lost(SchedulerInfo, EventFailure, #state{test_pid = TestPid} = State) ->
    reply(TestPid, {slave_lost, self(), SchedulerInfo, EventFailure}),
    {ok, State#state{callback = slave_lost}}.

executor_lost(SchedulerInfo, EventFailure,
              #state{test_pid = TestPid} = State) ->
    reply(TestPid, {executor_lost, self(), SchedulerInfo, EventFailure}),
    {ok, State#state{callback = executor_lost}}.

error(SchedulerInfo, EventError, #state{test_pid = TestPid} = State) ->
    reply(TestPid, {error, self(), SchedulerInfo, EventError}),
    {stop, State#state{callback = error}}.

handle_info(SchedulerInfo, teardown, #state{test_pid = TestPid} = State) ->
    Teardown = erl_mesos_scheduler:teardown(SchedulerInfo),
    reply(TestPid, {teardown, Teardown}),
    {stop, State};
handle_info(SchedulerInfo, {accept, OfferId, AgentId, TaskId},
            #state{test_pid = TestPid} = State) ->
    CommandValue = <<"while true; sleep 1; done">>,
    CommandInfo = #command_info{shell = true,
                                value = CommandValue},
    CpuScalarValue = #value_scalar{value = 0.1},
    ResourceCpu = #resource{name = <<"cpus">>,
                            type = <<"SCALAR">>,
                            scalar = CpuScalarValue},
    Labels = #labels{labels = [#label{key = <<"task_key">>,
                                      value = <<"task_value">>}]},
    TaskInfo = #task_info{name = <<"test_task">>,
                          task_id = TaskId,
                          agent_id = AgentId,
                          command = CommandInfo,
                          resources = [ResourceCpu],
                          labels = Labels},
    Launch = #offer_operation_launch{task_infos = [TaskInfo]},
    OfferOperation = #offer_operation{type = <<"LAUNCH">>,
                                      launch = Launch},
    Accept = erl_mesos_scheduler:accept(SchedulerInfo, [OfferId],
                                        [OfferOperation]),
    reply(TestPid, {accept, Accept}),
    {ok, State};
handle_info(SchedulerInfo, {decline, TaskId},
            #state{test_pid = TestPid} = State) ->
    Decline = erl_mesos_scheduler:decline(SchedulerInfo, [TaskId]),
    reply(TestPid, {decline, Decline}),
    {ok, State};
handle_info(SchedulerInfo, revive, #state{test_pid = TestPid} = State) ->
    Revive = erl_mesos_scheduler:revive(SchedulerInfo),
    reply(TestPid, {revive, Revive}),
    {ok, State};
handle_info(SchedulerInfo, {kill, TaskId},
            #state{test_pid = TestPid} = State) ->
    Kill = erl_mesos_scheduler:kill(SchedulerInfo, TaskId),
    reply(TestPid, {kill, Kill}),
    {ok, State};
handle_info(SchedulerInfo, {shutdown, ExecutorId, AgentId},
    #state{test_pid = TestPid} = State) ->
    Shutdown = erl_mesos_scheduler:shutdown(SchedulerInfo, ExecutorId, AgentId),
    reply(TestPid, {shutdown, Shutdown}),
    {ok, State};
handle_info(SchedulerInfo, {reconcile, TaskId},
            #state{test_pid = TestPid} = State) ->
    CallReconcileTask = #call_reconcile_task{task_id = TaskId},
    Reconcile = erl_mesos_scheduler:reconcile(SchedulerInfo,
                                              [CallReconcileTask]),
    reply(TestPid, {reconcile, Reconcile}),
    {ok, State};
handle_info(_SchedulerInfo, stop, State) ->
    {stop, State};
handle_info(_SchedulerInfo, _Info, State) ->
    {ok, State}.

terminate(SchedulerInfo, Reason, #state{test_pid = TestPid} = State) ->
    reply(TestPid, {terminate, self(), SchedulerInfo, Reason, State}).

%% Internal functions.

framework_info(Options) ->
    User = proplists:get_value(user, Options, <<>>),
    Name = proplists:get_value(name, Options, <<>>),
    Id = proplists:get_value(id, Options, undefined),
    FailoverTimeout = proplists:get_value(failover_timeout, Options, undefined),
    Checkpoint = proplists:get_value(checkpoint, Options, undefined),
    Role = proplists:get_value(role, Options, undefined),
    Hostname = proplists:get_value(hostname, Options, undefined),
    Principal = proplists:get_value(principal, Options, undefined),
    WebuiUrl = proplists:get_value(webui_url, Options, undefined),
    Capabilities = proplists:get_value(capabilities, Options, undefined),
    Labels = proplists:get_value(labels, Options, undefined),
    #framework_info{user = User,
                    name = Name,
                    id = Id,
                    failover_timeout = FailoverTimeout,
                    checkpoint = Checkpoint,
                    role = Role,
                    hostname = Hostname,
                    principal = Principal,
                    webui_url = WebuiUrl,
                    capabilities = Capabilities,
                    labels = Labels}.

reply(undefined, _Message) ->
    undefined;
reply(TestPid, Message) ->
    TestPid ! Message.

-module(erl_mesos_test_scheduler).

-behaviour(erl_mesos_scheduler).

-include_lib("scheduler_info.hrl").

-include_lib("scheduler_protobuf.hrl").

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

-record(state, {user,
                callback,
                test_pid}).

%% erl_mesos_scheduler callback functions.

init(Options) ->
    FrameworkInfo = framework_info(Options),
    TestPid = proplists:get_value(test_pid, Options),
    {ok, FrameworkInfo, true, #state{user = FrameworkInfo#'FrameworkInfo'.user,
                                     callback = init,
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
    CommandInfo = erl_mesos_utils:command_info("while true; sleep 1; done"),
    ResourceCpu = erl_mesos_utils:scalar_resource("cpus", 0.1),
    TaskInfo = #'TaskInfo'{name = "test_task",
                           task_id = TaskId,
                           agent_id = AgentId,
                           command = CommandInfo,
                           resources = [ResourceCpu]},
    Launch = #'Offer.Operation.Launch'{task_infos = [TaskInfo]},
    OfferOperation = #'Offer.Operation'{type = 'LAUNCH',
                                        launch = Launch},
    Accept = erl_mesos_scheduler:accept(SchedulerInfo, [OfferId],
                                        [OfferOperation]),
    reply(TestPid, {accept, Accept}),
    {ok, State};
handle_info(#scheduler_info{framework_id = FrameworkId} = SchedulerInfo,
            {accept_test_executor, OfferId, AgentId, TaskId},
            #state{user = User, test_pid = TestPid} = State) ->
    CommandInfoUris =
        [erl_mesos_utils:command_info_uri("test_executor"),
         erl_mesos_utils:command_info_uri("test_executor.py", false)],
    CommandInfo = erl_mesos_utils:command_info("./test_executor",
                                               CommandInfoUris, true, User),
    ExecutorResourceCpus = erl_mesos_utils:scalar_resource("cpus", 0.1),
    ExecutorId = erl_mesos_utils:executor_id(TaskId#'TaskID'.value),
    ExecutorInfo = erl_mesos_utils:executor_info(ExecutorId, CommandInfo,
                                                 [ExecutorResourceCpus],
                                                 FrameworkId),
    TaskResourceCpu = erl_mesos_utils:scalar_resource("cpus", 0.1),
    TaskInfo = #'TaskInfo'{name = "test_task",
                           task_id = TaskId,
                           agent_id = AgentId,
                           executor = ExecutorInfo,
                           resources = [TaskResourceCpu]},
    Launch = #'Offer.Operation.Launch'{task_infos = [TaskInfo]},
    OfferOperation = #'Offer.Operation'{type = 'LAUNCH',
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
handle_info(SchedulerInfo, {acknowledge, AgentId, TaskId, Uuid},
            #state{test_pid = TestPid} = State) ->
    Acknowledge =
        erl_mesos_scheduler:acknowledge(SchedulerInfo, AgentId, TaskId, Uuid),
    reply(TestPid, {acknowledge, Acknowledge}),
    {ok, State};
handle_info(SchedulerInfo, {reconcile, TaskId},
            #state{test_pid = TestPid} = State) ->
    CallReconcileTask = #'Call.Reconcile.Task'{task_id = TaskId},
    Reconcile = erl_mesos_scheduler:reconcile(SchedulerInfo,
                                              [CallReconcileTask]),
    reply(TestPid, {reconcile, Reconcile}),
    {ok, State};
handle_info(SchedulerInfo, {message, AgentId, ExecutorId, Data},
            #state{test_pid = TestPid} = State) ->
    Message =
        erl_mesos_scheduler:message(SchedulerInfo, AgentId, ExecutorId, Data),
    reply(TestPid, {message, Message}),
    {ok, State};
handle_info(SchedulerInfo, {request, Requests},
            #state{test_pid = TestPid} = State) ->
    Request = erl_mesos_scheduler:request(SchedulerInfo, Requests),
    reply(TestPid, {request, Request}),
    {ok, State};
handle_info(SchedulerInfo, suppress,
            #state{test_pid = TestPid} = State) ->
    Suppress = erl_mesos_scheduler:suppress(SchedulerInfo),
    reply(TestPid, {suppress, Suppress}),
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
    FailoverTimeout = proplists:get_value(failover_timeout, Options, undefined),
    #'FrameworkInfo'{user = User,
                     name = Name,
                     failover_timeout = FailoverTimeout}.

reply(undefined, _Message) ->
    undefined;
reply(TestPid, Message) ->
    TestPid ! Message.

-module(erl_mesos_test_master).

-include_lib("master_info.hrl").

-include_lib("master_protobuf.hrl").

-export([init/1,
         subscribed/3,
         disconnected/2,
         task_added/3,
         task_updated/3,
         handle_info/3,
         terminate/3]).

-record(state, {test_pid}).

%% erl_mesos_master callback functions.

init(Options) ->
    TestPid = proplists:get_value(test_pid, Options),
    {ok, #state{test_pid = TestPid}}.

subscribed(MasterInfo, EventSubscribed,
           #state{test_pid = TestPid} = State) ->
    reply(TestPid, subscribed, {self(), MasterInfo, EventSubscribed}),
    {ok, State}.

task_added(MasterInfo, EventTaskAdded,
           #state{test_pid = TestPid} = State) ->
    reply(TestPid, task_added, {self(), MasterInfo, EventTaskAdded}),
    {ok, State}.

task_updated(MasterInfo, EventTaskUpdated,
           #state{test_pid = TestPid} = State) ->
    reply(TestPid, task_updated, {self(), MasterInfo, EventTaskUpdated}),
    {ok, State}.

disconnected(MasterInfo, #state{test_pid = TestPid} = State) ->
    reply(TestPid, disconnected, {self(), MasterInfo}),
    {ok, State}.

handle_info(MasterInfo, get_version, #state{test_pid = TestPid} = State) ->
    Data = erl_mesos_master:get_version(MasterInfo),
    reply(TestPid, get_version, Data),
    {ok, State};
handle_info(MasterInfo, get_state, #state{test_pid = TestPid} = State) ->
    Data = erl_mesos_master:get_state(MasterInfo),
    reply(TestPid, get_state, Data),
    {ok, State};
handle_info(MasterInfo, get_agents, #state{test_pid = TestPid} = State) ->
    Data = erl_mesos_master:get_agents(MasterInfo),
    reply(TestPid, get_agents, Data),
    {ok, State};
handle_info(MasterInfo, get_frameworks, #state{test_pid = TestPid} = State) ->
    Data = erl_mesos_master:get_frameworks(MasterInfo),
    reply(TestPid, get_frameworks, Data),
    {ok, State};
handle_info(MasterInfo, get_executors, #state{test_pid = TestPid} = State) ->
    Data = erl_mesos_master:get_executors(MasterInfo),
    reply(TestPid, get_executors, Data),
    {ok, State};
handle_info(MasterInfo, get_tasks, #state{test_pid = TestPid} = State) ->
    Data = erl_mesos_master:get_tasks(MasterInfo),
    reply(TestPid, get_tasks, Data),
    {ok, State};
handle_info(MasterInfo, get_roles, #state{test_pid = TestPid} = State) ->
    Data = erl_mesos_master:get_roles(MasterInfo),
    reply(TestPid, get_roles, Data),
    {ok, State};
handle_info(MasterInfo, get_master, #state{test_pid = TestPid} = State) ->
    Data = erl_mesos_master:get_master(MasterInfo),
    reply(TestPid, get_master, Data),
    {ok, State};
handle_info(MasterInfo, get_master_info, #state{test_pid = TestPid} = State) ->
    Data = MasterInfo,
    reply(TestPid, get_master_info, Data),
    {ok, State};
handle_info(_MasterInfo, stop, State) ->
    {stop, State};
handle_info(_MasterInfo, _Info, State) ->
    {ok, State}.

terminate(MasterInfo, Reason, #state{test_pid = TestPid} = State) ->
    reply(TestPid, terminate, {self(), MasterInfo, Reason, State}).

%% Internal functions.

reply(undefined, _Name, _Message) ->
    undefined;
reply(TestPid, Name, Data) ->
    TestPid ! {Name, Data}.

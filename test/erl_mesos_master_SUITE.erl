-module(erl_mesos_master_SUITE).

-include_lib("common_test/include/ct.hrl").

-include_lib("master_info.hrl").

-include_lib("master_protobuf.hrl").

-export([all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2]).

-export([subscribed/1,
         disconnected/1,
         get_version/1,
         get_state/1,
         get_agents/1,
         get_frameworks/1,
         get_executors/1,
         get_tasks/1,
         get_roles/1,
         get_master/1,
         task_added/1,
         task_updated/1]).

-define(LOG, false).

all() ->
    [{group, mesos_cluster, [sequence]}].

groups() ->
    [{mesos_cluster, [subscribed,
                      disconnected,
                      task_added,
                      task_updated,
                      get_version,
                      get_state,
                      get_agents,
                      get_frameworks,
                      get_executors,
                      get_tasks,
                      get_roles,
                      get_master]}].

init_per_suite(Config) ->
    ok = erl_mesos:start(),
    Master = erl_mesos_test_master,
    MasterOptions = [],
    {ok, Masters} = erl_mesos_cluster:config(masters, Config),
    MasterHosts = [MasterHost || {_Container, MasterHost} <- Masters],
    Options = [{master_hosts, MasterHosts}],
    [{log, ?LOG},
     {master, Master},
     {master_options, MasterOptions},
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

subscribed(Config) ->
    log("Subscribed test cases.", Config),
    Ref = {erl_mesos_master, subscribed},
    Master = ?config(master, Config),
    MasterOptions = ?config(master_options, Config),
    MasterOptions1 = set_test_pid(MasterOptions),
    Options = ?config(options, Config),
    {ok, _} = start_master(Ref, Master, MasterOptions1, Options,
                              Config),
    {subscribed, {_, MasterInfo, EventSubscribed}} = recv_reply(subscribed),
    %% Test master info.
    #master_info{master_host = MasterHost,
                    subscribed = true} = MasterInfo,
    MasterHosts = proplists:get_value(master_hosts, Options),
    true = lists:member(binary_to_list(MasterHost), MasterHosts),
    %% Test event subscribed.
    #'Event.Subscribed'{get_state = _State} =
        EventSubscribed,
    ok = stop_master(Ref, Config).

disconnected(Config) ->
    log("Disconnected test cases.", Config),
    Ref = {erl_mesos_master, disconnected},
    Master = ?config(master, Config),
    MasterOptions = ?config(master_options, Config),
    MasterOptions1 = set_test_pid(MasterOptions),
    Options = ?config(options, Config),
    Options1 = [{max_num_resubscribe, 0} | Options],
    %% Test connection crash.
    {ok, _} = start_master(Ref, Master, MasterOptions1, Options1,
                              Config),
    {subscribed, {MasterPid, _, _}} = recv_reply(subscribed),
    Pid = response_pid(),
    exit(Pid, kill),
    {disconnected, {MasterPid, MasterInfo}} = recv_reply(disconnected),
    %% Test master info.
    #master_info{master_host = MasterHost,
                 subscribed = false} = MasterInfo,
    MasterHosts = proplists:get_value(master_hosts, Options),
    true = lists:member(binary_to_list(MasterHost), MasterHosts),
    %% Test cluster stop.
    Ref1 = {erl_mesos_master, disconnected, 1},
    {ok, _} = start_master(Ref1, Master, MasterOptions1, Options1,
                           Config),
    {subscribed, {MasterPid1, _, _}} = recv_reply(subscribed),
    erl_mesos_cluster:stop(Config),
    {disconnected, {MasterPid1, MasterInfo1}} = recv_reply(disconnected),
    %% Test master info.
    #master_info{master_host = MasterHost1,
                 subscribed = false} = MasterInfo1,
    true = lists:member(binary_to_list(MasterHost1), MasterHosts).

task_added(Config) ->
    log("Task Added test cases.", Config),
    Ref = {erl_mesos_master, subscribed},
    Master = ?config(master, Config),
    MasterOptions = ?config(master_options, Config),
    MasterOptions1 = set_test_pid(MasterOptions),
    Options = ?config(options, Config),
    {ok, _} = start_master(Ref, Master, MasterOptions1, Options,
                           Config),
    {subscribed, {_, _MasterInfo, _EventSubscribed}} = recv_reply(subscribed),
    %% Get task added...
    %% Test event task added.
    %% #'Event.TaskAdded'{task = _Task} =
    %%     EventTaskAdded,
    ok = stop_master(Ref, Config).

task_updated(Config) ->
    log("Task Updated test cases.", Config),
    Ref = {erl_mesos_master, subscribed},
    Master = ?config(master, Config),
    MasterOptions = ?config(master_options, Config),
    MasterOptions1 = set_test_pid(MasterOptions),
    Options = ?config(options, Config),
    {ok, _} = start_master(Ref, Master, MasterOptions1, Options,
                           Config),
    {subscribed, {_, _MasterInfo, _EventSubscribed}} = recv_reply(subscribed),
    %% Get task updated...
    %% Test event task updated.
    %% #'Event.TaskUpdated'{} =
    %%     EventTaskUpdated,
    ok = stop_master(Ref, Config).

get_version(Config) ->
    log("Get version test cases.", Config),
    Ref = {erl_mesos_master, get_version},
    Master = ?config(master, Config),
    MasterOptions = ?config(master_options, Config),
    MasterOptions1 = set_test_pid(MasterOptions),
    Options = ?config(options, Config),
    {ok, _} = start_master(Ref, Master, MasterOptions1, Options,
                              Config),
    {subscribed, {MasterPid, _, _}} = recv_reply(subscribed),
    MasterPid ! get_version,
    {get_version, #'Response'{}} = recv_reply(get_version),
    MasterPid ! get_master_info,
    {get_master_info, MasterInfo} =
        recv_reply(get_master_info),
    %% Test master info.
    #master_info{subscribed = true} = MasterInfo,
    ok = stop_master(Ref, Config).

get_state(Config) ->
    log("Get state test cases.", Config),
    Ref = {erl_mesos_master, get_state},
    Master = ?config(master, Config),
    MasterOptions = ?config(master_options, Config),
    MasterOptions1 = set_test_pid(MasterOptions),
    Options = ?config(options, Config),
    {ok, _} = start_master(Ref, Master, MasterOptions1, Options,
                              Config),
    {subscribed, {MasterPid, _, _}} = recv_reply(subscribed),
    MasterPid ! get_state,
    {get_state, #'Response'{}} = recv_reply(get_state),
    ok = stop_master(Ref, Config).

get_agents(Config) ->
    log("Get agents test cases.", Config),
    Ref = {erl_mesos_master, get_agents},
    Master = ?config(master, Config),
    MasterOptions = ?config(master_options, Config),
    MasterOptions1 = set_test_pid(MasterOptions),
    Options = ?config(options, Config),
    {ok, _} = start_master(Ref, Master, MasterOptions1, Options,
                              Config),
    {subscribed, {MasterPid, _, _}} = recv_reply(subscribed),
    MasterPid ! get_agents,
    {get_agents, #'Response'{}} = recv_reply(get_agents),
    ok = stop_master(Ref, Config).

get_frameworks(Config) ->
    log("Get frameworks test cases.", Config),
    Ref = {erl_mesos_master, get_frameworks},
    Master = ?config(master, Config),
    MasterOptions = ?config(master_options, Config),
    MasterOptions1 = set_test_pid(MasterOptions),
    Options = ?config(options, Config),
    {ok, _} = start_master(Ref, Master, MasterOptions1, Options,
                              Config),
    {subscribed, {MasterPid, _, _}} = recv_reply(subscribed),
    MasterPid ! get_frameworks,
    {get_frameworks, #'Response'{}} = recv_reply(get_frameworks),
    ok = stop_master(Ref, Config).

get_executors(Config) ->
    log("Get executors test cases.", Config),
    Ref = {erl_mesos_master, get_executors},
    Master = ?config(master, Config),
    MasterOptions = ?config(master_options, Config),
    MasterOptions1 = set_test_pid(MasterOptions),
    Options = ?config(options, Config),
    {ok, _} = start_master(Ref, Master, MasterOptions1, Options,
                              Config),
    {subscribed, {MasterPid, _, _}} = recv_reply(subscribed),
    MasterPid ! get_executors,
    {get_executors, #'Response'{}} = recv_reply(get_executors),
    ok = stop_master(Ref, Config).

get_tasks(Config) ->
    log("Get tasks test cases.", Config),
    Ref = {erl_mesos_master, get_tasks},
    Master = ?config(master, Config),
    MasterOptions = ?config(master_options, Config),
    MasterOptions1 = set_test_pid(MasterOptions),
    Options = ?config(options, Config),
    {ok, _} = start_master(Ref, Master, MasterOptions1, Options,
                              Config),
    {subscribed, {MasterPid, _, _}} = recv_reply(subscribed),
    MasterPid ! get_tasks,
    {get_tasks, #'Response'{}} = recv_reply(get_tasks),
    ok = stop_master(Ref, Config).

get_roles(Config) ->
    log("Get roles test cases.", Config),
    Ref = {erl_mesos_master, get_roles},
    Master = ?config(master, Config),
    MasterOptions = ?config(master_options, Config),
    MasterOptions1 = set_test_pid(MasterOptions),
    Options = ?config(options, Config),
    {ok, _} = start_master(Ref, Master, MasterOptions1, Options,
                              Config),
    {subscribed, {MasterPid, _, _}} = recv_reply(subscribed),
    MasterPid ! get_roles,
    {get_roles, #'Response'{}} = recv_reply(get_roles),
    ok = stop_master(Ref, Config).

get_master(Config) ->
    log("Get master test cases.", Config),
    Ref = {erl_mesos_master, get_master},
    Master = ?config(master, Config),
    MasterOptions = ?config(master_options, Config),
    MasterOptions1 = set_test_pid(MasterOptions),
    Options = ?config(options, Config),
    {ok, _} = start_master(Ref, Master, MasterOptions1, Options,
                              Config),
    {subscribed, {MasterPid, _, _}} = recv_reply(subscribed),
    MasterPid ! get_master,
    {get_master, #'Response'{}} = recv_reply(get_master),
    ok = stop_master(Ref, Config).

%% Internal functions.

start_mesos_cluster(Config) ->
    log("Start test mesos cluster.", Config),
    erl_mesos_cluster:start(Config),
    {ok, StartTimeout} = erl_mesos_cluster:config(start_timeout, Config),
    timer:sleep(StartTimeout).

stop_mesos_cluster(Config) ->
    log("Stop test mesos cluster.", Config),
    erl_mesos_cluster:stop(Config).

start_master(Ref, Master, MasterOptions, Options, Config) ->
    log("Start master. Ref: ~p, Master: ~p.", [Ref, Master], Config),
    erl_mesos:start_master(Ref, Master, MasterOptions, Options).

stop_master(Ref, Config) ->
    log("Stop scheduler. Ref: ~p.", [Ref], Config),
    erl_mesos:stop_master(Ref).

set_test_pid(MasterOptions) ->
    [{test_pid, self()} | MasterOptions].

response_pid() ->
    [{ClientRef, _Request} | _] = ets:tab2list(hackney_manager),
    {ok, Pid} = hackney_manager:async_response_pid(ClientRef),
    Pid.

recv_reply(Reply) ->
    erl_mesos_test_utils:recv_reply(Reply).

log(Format, Config) ->
    log(Format, [], Config).

log(Format, Data, Config) ->
    case ?config(log, Config) of
        true ->
            ct:pal(Format, Data);
        false ->
            ok
    end.

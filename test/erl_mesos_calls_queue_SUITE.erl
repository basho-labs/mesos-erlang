%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies Inc. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License. You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied. See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(erl_mesos_calls_queue_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0]).

-export([ok_call_fun/0, error_call_fun/1]).

-export([pop_push_call/1, exec_or_push_call/1, exec_calls/1]).

all() ->
    [pop_push_call, exec_or_push_call, exec_calls].

ok_call_fun() ->
    ok.

error_call_fun(Reason) ->
    {error, Reason}.

%% Test functions.

pop_push_call(_Config) ->
    %% Push.
    MaxLen = 100,
    CallsQueue = erl_mesos_calls_queue:new([{max_len, MaxLen}]),
    0 = erl_mesos_calls_queue:len(CallsQueue),
    Numbers = lists:seq(1, MaxLen),
    PushFun = fun(Num, Queue) ->
                  {ok, Queue1} =
                      erl_mesos_calls_queue:push_call(error_call(Num), Queue),
                  Queue1
              end,
    CallsQueue1 = lists:foldl(PushFun, CallsQueue, Numbers),
    MaxLen = erl_mesos_calls_queue:len(CallsQueue1),
    {error, calls_queue_len_limit} =
        erl_mesos_calls_queue:push_call(error_call(MaxLen + 1), CallsQueue1),
    %% Pop.
    PopFun = fun(Num, Queue) ->
                 {ok, {?MODULE, error_call_fun, [{bad_call, Num}]}, Queue1} =
                      erl_mesos_calls_queue:pop_call(Queue),
                 Queue1
             end,
    CallsQueue2 = lists:foldl(PopFun, CallsQueue1, Numbers),
    0 = erl_mesos_calls_queue:len(CallsQueue2),
    calls_queue_empty = erl_mesos_calls_queue:pop_call(CallsQueue2).

exec_or_push_call(_Config) ->
    %% Execute.
    MaxLen = 10,
    CallsQueue = erl_mesos_calls_queue:new([{max_len, MaxLen}]),
    0 = erl_mesos_calls_queue:len(CallsQueue),
    {ok, CallsQueue} =
        erl_mesos_calls_queue:exec_or_push_call(ok_call(), CallsQueue),
    %% Push.
    PushFun = fun(Num, Queue) ->
                  {exec_error, {bad_call, Num}, Queue1} =
                      erl_mesos_calls_queue:exec_or_push_call(error_call(Num),
                                                              Queue),
                  Queue1
              end,
    CallsQueue1 = lists:foldl(PushFun, CallsQueue, lists:seq(1, MaxLen)),
    MaxLen = erl_mesos_calls_queue:len(CallsQueue1),
    {error, calls_queue_len_limit} =
        erl_mesos_calls_queue:exec_or_push_call(error_call(MaxLen + 1),
                                                CallsQueue1).

exec_calls(_Config) ->
    MaxLen = 5,
    MaxNumTryExecute = 3,
    Options = [{max_len, MaxLen}, {max_num_try_execute, MaxNumTryExecute}],
    CallsQueue = erl_mesos_calls_queue:new(Options),
    calls_queue_empty = erl_mesos_calls_queue:exec_calls(CallsQueue),
    %% Push.
    Numbers = lists:seq(1, MaxLen),
    [_ | Numbers1] = Numbers,
    {ok, CallsQueue1} = erl_mesos_calls_queue:push_call(ok_call(), CallsQueue),
    PushFun = fun(Num, Queue) ->
                  {ok, Queue1} =
                      erl_mesos_calls_queue:push_call(error_call(Num), Queue),
                  Queue1
              end,
    CallsQueue2 = lists:foldl(PushFun, CallsQueue1, Numbers1),
    %% Exec ok.
    MaxLen = erl_mesos_calls_queue:len(CallsQueue2),
    [Num | _] = Numbers1,
    {exec_error, {bad_call, Num}, CallsQueue3} =
        erl_mesos_calls_queue:exec_calls(CallsQueue2),
    ExecLen = MaxLen - 1,
    ExecLen = erl_mesos_calls_queue:len(CallsQueue3),
    %% Exec error.
    ExecFun = fun(_, Queue) ->
                  {exec_error, {bad_call, _}, Queue1} =
                      erl_mesos_calls_queue:exec_calls(Queue),
                  Queue1
              end,
    CallsQueue4 = lists:foldl(ExecFun, CallsQueue3,
                              lists:seq(1, MaxNumTryExecute)),
    {error, calls_queue_try_execute_limit} =
        erl_mesos_calls_queue:exec_calls(CallsQueue4).

%% Internal functions.

ok_call() ->
    {?MODULE, ok_call_fun, []}.

error_call(Num) ->
    {?MODULE, error_call_fun, [{bad_call, Num}]}.

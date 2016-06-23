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

-module(erl_mesos_test_utils).

-include_lib("scheduler_protobuf.hrl").

-export([offers/1]).

-export([timestamp_task_id/0]).

-export([response_pid/0]).

-export([recv_reply/1, recv_framework_message_reply/1]).

-define(RECV_REPLY_TIMEOUT, 10000).

%% External functions.

offers(#'Event.Offers'{offers = Offers}) ->
    Offers.

timestamp_task_id() ->
    {MegaSecs, Secs, MicroSecs} = os:timestamp(),
    Timestamp = (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs,
    #'TaskID'{value = integer_to_list(Timestamp)}.

response_pid() ->
    [{ClientRef, _Request} | _] = ets:tab2list(hackney_manager),
    {ok, Pid} = hackney_manager:async_response_pid(ClientRef),
    Pid.

recv_reply(Reply) ->
    receive
        {Reply, Data} ->
            {Reply, Data}
    after ?RECV_REPLY_TIMEOUT ->
        {error, timeout}
    end.

recv_framework_message_reply(Reply) ->
    {framework_message, {_, _, #'Event.Message'{data = MessageData}}} =
        recv_reply(framework_message),
    case binary_to_term(MessageData) of
        {Reply, Data} ->
            {Reply, Data};
        _FrameworkMessageReply ->
            recv_framework_message_reply(Reply)
    end.

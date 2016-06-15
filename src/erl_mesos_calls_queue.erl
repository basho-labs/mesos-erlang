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

-module(erl_mesos_calls_queue).

-export([new/0,
         new/1,
         len/1,
         pop_call/1,
         push_call/2,
         exec_or_push_call/2,
         exec_calls/1]).

-record(calls_queue, {calls :: queue:queue(),
                      max_len :: pos_integer(),
                      max_num_try_execute :: pos_integer(),
                      num_try_execute = 0 :: non_neg_integer()}).

-type calls_queue() :: #calls_queue{}.
-export_type([calls_queue/0]).

-type call() :: {module(), atom(), [term()]}.
-export_type([call/0]).

-define(DEFAULT_MAX_LEN, 10).

-define(DEFAULT_MAX_NUM_TRY_EXECUTE, 5).

%% External functions.

%% @equiv new()
-spec new() -> calls_queue().
new() ->
    new([]).

%% @doc Creates new queue.
-spec new([{atom(), term()}]) -> calls_queue().
new(Options) ->
    MaxLen = proplists:get_value(max_len, Options, ?DEFAULT_MAX_LEN),
    MaxNumTryExecute = proplists:get_value(max_num_try_execute, Options,
                                           ?DEFAULT_MAX_NUM_TRY_EXECUTE),
    #calls_queue{calls = queue:new(),
                 max_len = MaxLen,
                 max_num_try_execute = MaxNumTryExecute}.

-spec len(calls_queue()) -> non_neg_integer().
len(#calls_queue{calls = Calls}) ->
    queue:len(Calls).

%% @doc Popes call from the calls queue.
-spec pop_call(calls_queue()) ->
    {ok, call(), calls_queue()} | calls_queue_empty.
pop_call(#calls_queue{calls = Calls} = CallsQueue) ->
    case queue:len(Calls) > 0 of
        true ->
            {{value, Call}, Calls1} = queue:out(Calls),
            {ok, Call, CallsQueue#calls_queue{calls = Calls1}};
        false ->
            calls_queue_empty
    end.

%% @doc Pushes call to the calls queue.
-spec push_call(call(), calls_queue()) ->
    {ok, calls_queue()} | {error, calls_queue_len_limit}.
push_call(Call, #calls_queue{calls = Calls,
                             max_len = MaxLen} = CallsQueue) ->
    case queue:len(Calls) < MaxLen of
        true ->
            {ok, CallsQueue#calls_queue{calls = queue:in(Call, Calls)}};
        false ->
            {error, calls_queue_len_limit}
    end.

%% @doc Executes call or put call to the calls queue.
-spec exec_or_push_call(call(), calls_queue()) ->
    {ok, calls_queue()} | {exec_error, term(), calls_queue()} |
    {error, calls_queue_len_limit}.
exec_or_push_call(Call, CallsQueue) ->
    case execute_call(Call) of
        ok ->
            {ok, CallsQueue};
        {error, Reason} ->
            case push_call(Call, CallsQueue) of
                {ok, CallsQueue1} ->
                    {exec_error, Reason, CallsQueue1};
                {error, Reason1} ->
                    {error, Reason1}
            end
    end.

%% @doc Executes calls form the calls queue.
-spec exec_calls(calls_queue()) ->
    {exec_error, term(), calls_queue()} | {error, term()} | calls_queue_empty.
exec_calls(#calls_queue{max_num_try_execute = MaxNumTryExecute,
                        num_try_execute = NumTryExecute})
  when MaxNumTryExecute + 1 =:= NumTryExecute ->
    {error, calls_queue_try_execute_limit};
exec_calls(#calls_queue{calls = Calls,
                        num_try_execute = NumTryExecute} = CallsQueue) ->
    case queue:len(Calls) > 0 of
        true ->
            Call = queue:get(Calls),
            case execute_call(Call) of
                ok ->
                    Calls1 = queue:drop(Calls),
                    CallsQueue1 = CallsQueue#calls_queue{calls = Calls1,
                                                         num_try_execute = 0},
                    exec_calls(CallsQueue1);
                {error, Reason} ->
                    CallsQueue1 = CallsQueue#calls_queue{num_try_execute =
                                                         NumTryExecute + 1},
                    {exec_error, Reason, CallsQueue1}
            end;
        false ->
            calls_queue_empty
    end.

%% Internal functions.

%% @doc Executes call.
%% @private
-spec execute_call(call()) -> ok | {error, term()}.
execute_call({Module, Function, Args}) ->
    erlang:apply(Module, Function, Args).

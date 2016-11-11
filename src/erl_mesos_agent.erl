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

-module(erl_mesos_agent).

-behaviour(gen_server).

-include("agent_info.hrl").

-include("agent_protobuf.hrl").

-export([start_link/4]).

-export([get_version/1,
         get_state/1,
         get_frameworks/1,
         get_executors/1,
         get_tasks/1,
         get_containers/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3,
         format_status/2]).

-record(state, {ref :: term(),
                agent :: module(),
                data_format :: erl_mesos_data_format:data_format(),
                data_format_module :: module(),
                api_version :: erl_mesos_agent_call:version(),
                agent_host :: binary(),
                request_options :: erl_mesos_http:options(),
                agent_state :: undefined | term()}).

-type options() :: [{atom(), term()}].
-export_type([options/0]).

-type 'Call'() :: #'Call'{}.
-export_type(['Call'/0]).

-type 'Call.GetMetrics'() :: #'Call.GetMetrics'{}.
-export_type(['Call.GetMetrics'/0]).

-type 'Call.SetLoggingLevel'() :: #'Call.SetLoggingLevel'{}.
-export_type(['Call.SetLoggingLevel'/0]).

-type 'Call.ListFiles'() :: #'Call.ListFiles'{}.
-export_type(['Call.ListFiles'/0]).

-type 'Call.ReadFile'() :: #'Call.ReadFile'{}.
-export_type(['Call.ReadFile'/0]).

-type agent_info() :: #agent_info{}.
-export_type([agent_info/0]).

-type state() :: #state{}.

%% Callbacks.

-callback init(term()) ->
    {ok, term()} | {stop, term()}.

-callback handle_info(agent_info(), term(), term()) ->
    {ok, term()} | {stop, term()}.

-callback terminate(agent_info(), term(), term()) -> term().

-define(DEFAULT_AGENT_HOST, <<"localhost:5051">>).

-define(DEFAULT_REQUEST_OPTIONS, []).

-define(DEFAULT_RECV_TIMEOUT, 5000).

-define(DEFAULT_MAX_REDIRECT, 5).

-define(DEFAULT_MAX_NUM_RESUBSCRIBE, 1).

-define(DEFAULT_RESUBSCRIBE_INTERVAL, 0).

-define(DATA_FORMAT, protobuf).

-define(DATA_FORMAT_MODULE, master_protobuf).

-define(API_VERSION, v1).

%% External functions.

%% @doc Starts the `erl_mesos_master' process.
-spec start_link(term(), module(), term(), options()) ->
    {ok, pid()} | {error, term()}.
start_link(Ref, Agent, AgentOptions, Options) ->
    gen_server:start_link(?MODULE, {Ref, Agent, AgentOptions, Options},
                          []).

%% @doc GetVersion call.
-spec get_version(agent_info()) -> term() | {error, term()}.
get_version(AgentInfo) ->
    erl_mesos_agent_call:get_version(AgentInfo).

%% @doc GetState call.
-spec get_state(agent_info()) -> term() | {error, term()}.
get_state(AgentInfo) ->
    erl_mesos_agent_call:get_state(AgentInfo).

%% @doc GetFrameworks call.
-spec get_frameworks(agent_info()) -> term() | {error, term()}.
get_frameworks(AgentInfo) ->
    erl_mesos_agent_call:get_frameworks(AgentInfo).

%% @doc GetExecutors call.
-spec get_executors(agent_info()) -> term() | {error, term()}.
get_executors(AgentInfo) ->
    erl_mesos_agent_call:get_executors(AgentInfo).

%% @doc GetTasks call.
-spec get_tasks(agent_info()) -> term() | {error, term()}.
get_tasks(AgentInfo) ->
    erl_mesos_agent_call:get_tasks(AgentInfo).

%% @doc GetContainers call.
-spec get_containers(agent_info()) -> term() | {error, term()}.
get_containers(AgentInfo) ->
    erl_mesos_agent_call:get_containers(AgentInfo).

%% GET_HEALTH
%% GET_FLAGS
%% GET_METRICS
%% GET_LOGGING_LEVEL
%% SET_LOGGING_LEVEL
%% LIST_FILES
%% READ_FILE
%% LAUNCH_NESTED_CONTAINER
%% WAIT_NESTED_CONTAINER
%% KILL_NESTED_CONTAINER
%% LAUNCH_NESTED_CONTAINER_SESSION
%% ATTACH_CONTAINER_INPUT
%% ATTACH_CONTAINER_OUTPUT

%% gen_server callback functions.

%% @private
-spec init({term(), module(), term(), options()}) ->
    {ok, state()} | {stop, term()}.
init({Ref, Agent, AgentOptions, Options}) ->
    case init(Ref, Agent, AgentOptions, Options) of
        {ok, State} ->
            {ok, State};
        {error, Reason} ->
            {stop, Reason}
    end.

%% @private
-spec handle_call(term(), {pid(), term()}, state()) -> {noreply, state()}.
handle_call(Request, _From, State) ->
    log_warning("Agent received unexpected call request.", "Request: ~p.",
                [Request], State),
    {noreply, State}.

%% @private
-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(Request, State) ->
    log_warning("Agent received unexpected cast request.", "Request: ~p.",
                [Request], State),
    {noreply, State}.

%% @private
-spec handle_info(term(), state()) ->
    {noreply, state()} | {stop, term(), state()}.
handle_info(Info, State) ->
    log_warning("Agent received unexpected handle info request.", "Request: ~p.",
                [Info], State),
    {noreply, State}.

%% @private
-spec terminate(term(), state()) -> term().
terminate(Reason, #state{agent = Agent,
                         agent_state = AgentState} = State) ->
    AgentInfo = agent_info(State),
    Agent:terminate(AgentInfo, Reason, AgentState).

%% @private
-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
-spec format_status(normal | terminate, [{atom(), term()} | state()]) ->
    [{atom(), term()}].
format_status(normal, [_Dict, State]) ->
    [{data, [{"State", format_state(State)}]}];
format_status(terminate, [_Dict, State]) ->
    format_state(State).

%% Internal functions.

%% @doc Validates options and sets options to the state.
%% @private
-spec init(term(), module(), term(), options()) ->
    {ok, state()} | {error, term()}.
init(Ref, Agent, AgentOptions, Options) ->
    Funs = [fun agent_host/1,
            fun request_options/1],
    case options(Funs, Options) of
        {ok, ValidOptions} ->
            State = state(Ref, Agent, ValidOptions),
            case init(AgentOptions, State) of
                {ok, State1} ->
                    {ok, State1};
                {stop, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Returns agent host.
%% @private
-spec agent_host(options()) ->
    {ok, {agent_host, [binary()]}} | {error, {bad_agent_host, term()}} |
    {error, {bad_agent_host, term()}}.
agent_host(Options) ->
    case proplists:get_value(agent_host, Options, ?DEFAULT_AGENT_HOST) of
        AgentHost when is_list(AgentHost) -> list_to_binary(AgentHost);
        AgentHost when is_binary(AgentHost) -> AgentHost;
        AgentHosts ->
            {error, {bad_agent_host, AgentHosts}}
    end.

%% @doc Returns request options.
%% @private
-spec request_options(options()) ->
    {ok, {request_options, [{atom(), term()}]}} |
    {error, {bad_request_options, term()}}.
request_options(Options) ->
    case proplists:get_value(request_options, Options,
                             ?DEFAULT_REQUEST_OPTIONS) of
        RequestOptions when is_list(RequestOptions) ->
            {ok, {request_options, RequestOptions}};
        RequestOptions ->
            {error, {bad_request_options, RequestOptions}}
    end.

%% @doc Returns validated options.
%% @private
-spec options([fun((options()) -> {ok, {atom(), term()}} | {error, term()})],
              options()) ->
    {ok, options()} | {error, term()}.
options(Funs, Options) when is_list(Options) ->
    options(Funs, Options, []);
options(_Funs, Options) ->
    {error, {bad_options, Options}}.

%% @doc Returns validated options.
%% @private
-spec options([fun((options()) -> {ok, {atom(), term()}} | {error, term()})],
              options(), options()) ->
    {ok, options()} | {error, term()}.
options([Fun | Funs], Options, ValidOptions) ->
    case Fun(Options) of
        {ok, Option} ->
            options(Funs, Options, [Option | ValidOptions]);
        {error, Reason} ->
            {error, Reason}
    end;
options([], _Options, ValidOptions) ->
    {ok, ValidOptions}.

%% @doc Returns state.
%% @private
-spec state(term(), module(), options()) -> state().
state(Ref, Agent, Options) ->
    AgentHost = proplists:get_value(agent_host, Options),
    RequestOptions = proplists:get_value(request_options, Options),
    #state{ref = Ref,
           agent = Agent,
           data_format = ?DATA_FORMAT,
           data_format_module = ?DATA_FORMAT_MODULE,
           api_version = ?API_VERSION,
           agent_host = AgentHost,
           request_options = RequestOptions}.

%% @doc Calls Agent:init/1.
%% @private
-spec init(term(), state()) -> {ok, state()} | {stop, term()}.
init(AgentOptions, #state{agent = Agent} = State) ->
    case Agent:init(AgentOptions) of
        {ok, AgentState} ->
            {ok, State#state{agent_state = AgentState}};
        {stop, Reason} ->
            {stop, Reason}
    end.

%% @doc Returns master info.
%% @private
-spec agent_info(state()) -> agent_info().
agent_info(#state{data_format = DataFormat,
                   data_format_module = DataFormatModule,
                   api_version = ApiVersion,
                   agent_host = AgentHost,
                   request_options = RequestOptions}) ->
    #agent_info{data_format = DataFormat,
                 data_format_module = DataFormatModule,
                 api_version = ApiVersion,
                 agent_host = AgentHost,
                 request_options = RequestOptions}.

%% @doc Logs info.
%% @private
%% -spec log_info(string(), string(), [term()], state()) -> ok.
%% log_info(Message, Format, Data, #state{ref = Ref, agent = Agent}) ->
%%     erl_mesos_logger:info(Message ++ " Ref: ~p, Agent: ~p, " ++ Format,
%%                           [Ref, Agent | Data]).

%% @doc Logs warning.
%% @private
-spec log_warning(string(), string(), [term()], state()) -> ok.
log_warning(Message, Format, Data, #state{ref = Ref, agent = Agent}) ->
    erl_mesos_logger:warning(Message ++ " Ref: ~p, Agent: ~p, " ++ Format,
                             [Ref, Agent | Data]).

%% @doc Logs error.
%% @private
%% -spec log_error(string(), state()) -> ok.
%% log_error(Message, #state{ref = Ref, agent = Agent}) ->
%%     erl_mesos_logger:error(Message ++ " Ref: ~p, Agent: ~p.",
%%                            [Ref, Agent]).

%% @doc Logs error.
%% @private
%% -spec log_error(string(), string(), [term()], state()) -> ok.
%% log_error(Message, Format, Data, #state{ref = Ref, agent = Agent}) ->
%%     erl_mesos_logger:error(Message ++ " Ref: ~p, Agent: ~p, " ++ Format,
%%                            [Ref, Agent | Data]).

%% @doc Formats state.
%% @private
-spec format_state(state()) -> [{string(), [{atom(), term()}]}].
format_state(#state{ref = Ref,
                    agent = Agent,
                    data_format = DataFormat,
                    data_format_module = DataFormatModule,
                    api_version = ApiVersion,
                    agent_host = AgentHost,
                    request_options = RequestOptions,
                    agent_state = AgentState}) ->
    State = [{data_format, DataFormat},
             {data_format_module, DataFormatModule},
             {api_version, ApiVersion},
             {agent_host, AgentHost},
             {request_options, RequestOptions}],
    [{"Ref", Ref},
     {"Agent", Agent},
     {"Agent state", AgentState},
     {"State", State}].

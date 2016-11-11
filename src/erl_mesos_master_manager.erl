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

%% @privat

-module(erl_mesos_master_manager).

-behaviour(gen_server).

-export([start_link/0]).

-export([start_master/5, stop_master/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {monitors = [] :: [{{reference(), pid()}, term()}]}).

-type state() :: #state{}.

-define(TAB, erl_mesos_masters).

%% External functions.

%% @doc Starts the `erl_mesos_master_manager' process.
-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {}, []).

%% @doc Starts the `erl_mesos_master' process.
-spec start_master(term(), module(), term(),
                      erl_mesos_master:options(), timeout()) ->
    {ok, pid()} | {error, term()}.
start_master(Ref, Master, MasterOptions, Options, Timeout) ->
    gen_server:call(?MODULE, {start_master, Ref, Master, MasterOptions,
                              Options}, Timeout).

%% @doc Stops the `erl_mesos_master' process.
-spec stop_master(term()) -> ok | {error, term()}.
stop_master(Ref) ->
    gen_server:call(?MODULE, {stop_master, Ref}).

%% gen_server callback functions.

%% @private
-spec init({}) -> {ok, state()}.
init({}) ->
    Monitors = [{{erlang:monitor(process, Pid), Pid}, Ref} || [Ref, Pid] <-
                ets:match(erl_mesos_masters, {{'$1', pid}, '$2'})],
    {ok, #state{monitors = Monitors}}.

%% @private
-spec handle_call(term(), {pid(), term()}, state()) ->
    {reply, ok | {ok, pid()} | {error, term()}, state()} | {noreply, state()}.
handle_call({start_master, Ref, Master, MasterOptions, Options},
            _From, #state{monitors = Monitors} = State) ->
    case get_master_option(Ref, pid) of
        {ok, Pid} ->
            {reply, {error, {already_started, Pid}}, State};
        not_found ->
            case erl_mesos_master_sup:start_master(Ref, Master,
                                                         MasterOptions,
                                                         Options) of
                {ok, Pid} ->
                    set_master(Ref, Pid, Master, MasterOptions,
                                  Options),
                    Monitor = {{erlang:monitor(process, Pid), Pid}, Ref},
                    {reply, {ok, Pid},
                     State#state{monitors = [Monitor | Monitors]}};
                {error, Reason} ->
                    {reply, {error, Reason}, State}
            end
    end;
handle_call({stop_master, Ref}, _From, State) ->
    case get_master_option(Ref, pid) of
        {ok, Pid} ->
            case erl_mesos_master_sup:stop_master(Pid) of
                ok ->
                    {reply, ok, State};
                {error, Reason} ->
                    {reply, {error, Reason}, State}
            end;
        not_found ->
            {reply, {error, not_found}, State}
    end;
handle_call(Request, _From, State) ->
    erl_mesos_logger:warning("Master manager received unexpected call "
                             "request. Request: ~p.", [Request]),
    {noreply, State}.

%% @private
-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(Request, State) ->
    erl_mesos_logger:warning("Master manager received unexpected cast "
                             "request. Request: ~p.", [Request]),
    {noreply, State}.

%% @private
-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info({'DOWN', MonitorRef, process, Pid, _Reason},
            #state{monitors = Monitors} = State) ->
    {_, Ref} = lists:keyfind({MonitorRef, Pid}, 1, Monitors),
    Monitors1 = lists:keydelete({MonitorRef, Pid}, 1, Monitors),
    remove_master(Ref),
    {noreply, State#state{monitors = Monitors1}};
handle_info(Request, State) ->
    erl_mesos_logger:warning("Master manager received unexpected "
                             "message. Message: ~p.", [Request]),
    {noreply, State}.

%% @private
-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions.

%% @doc Returns master option.
%% @private
-spec get_master_option(term(), atom()) -> {ok, term()} | not_found.
get_master_option(Ref, Key) ->
    case ets:lookup(?TAB, {Ref, Key}) of
        [{{Ref, Key}, Value}] ->
            {ok, Value};
        [] ->
            not_found
    end.

%% @doc Sets master.
%% @private
-spec set_master(term(), pid(), module(), term(),
                    erl_mesos_master:options()) ->
    true.
set_master(Ref, Pid, Master, MasterOptions, Options) ->
    ets:insert(?TAB, {{Ref, pid}, Pid}),
    ets:insert(?TAB, {{Ref, master}, Master}),
    ets:insert(?TAB, {{Ref, master_options}, MasterOptions}),
    ets:insert(?TAB, {{Ref, options}, Options}).

%% @doc Removes master.
%% @private
-spec remove_master(reference()) -> true.
remove_master(Ref) ->
    ets:match_delete(?TAB, {{Ref, '$1'}, '$2'}).

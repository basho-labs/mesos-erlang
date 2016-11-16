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

-module(erl_mesos).

-include("erl_mesos_scheduler_proto.hrl").

-export([get_env_value/1,
         get_env_value/2,
         get_env_converted_value/2,
         get_env_converted_value/3]).

-type 'AgentID'() :: #'AgentID'{}.
-export_type(['AgentID'/0]).

-type 'CommandInfo'() :: #'CommandInfo'{}.
-export_type(['CommandInfo'/0]).

-type 'CommandInfo.URI'() :: #'CommandInfo.URI'{}.
-export_type(['CommandInfo.URI'/0]).

-type 'ExecutorID'() :: #'ExecutorID'{}.
-export_type(['ExecutorID'/0]).

-type 'ExecutorInfo'() :: #'ExecutorInfo'{}.
-export_type(['ExecutorInfo'/0]).

-type 'Filters'() :: #'Filters'{}.
-export_type(['Filters'/0]).

-type 'FrameworkID'() :: #'FrameworkID'{}.
-export_type(['FrameworkID'/0]).

-type 'FrameworkInfo'() :: #'FrameworkInfo'{}.
-export_type(['FrameworkInfo'/0]).

-type 'MasterInfo'() :: #'MasterInfo'{}.
-export_type(['MasterInfo'/0]).

-type 'Offer'() :: #'Offer'{}.
-export_type(['Offer'/0]).

-type 'Offer.Operation'() :: #'Offer.Operation'{}.
-export_type(['Offer.Operation'/0]).

-type 'OfferID'() :: #'OfferID'{}.
-export_type(['OfferID'/0]).

-type 'Resource'() :: #'Resource'{}.
-export_type(['Resource'/0]).

-type 'Request'() :: #'Request'{}.
-export_type(['Request'/0]).

-type 'TaskID'() :: #'TaskID'{}.
-export_type(['TaskID'/0]).

-type 'TaskInfo'() :: #'TaskInfo'{}.
-export_type(['TaskInfo'/0]).

-type 'TaskStatus'() :: #'TaskStatus'{}.
-export_type(['TaskStatus'/0]).

-type 'Value'() :: #'Value'{}.
-export_type(['Value'/0]).

-type 'Value.Scalar'() :: #'Value.Scalar'{}.
-export_type(['Value.Scalar'/0]).

-type 'Value.Ranges'() :: #'Value.Ranges'{}.
-export_type(['Value.Ranges'/0]).

-type 'Value.Range'() :: #'Value.Range'{}.
-export_type(['Value.Range'/0]).

-type 'Value.Set'() :: #'Value.Set'{}.
-export_type(['Value.Set'/0]).

-type 'Value.Text'() :: #'Value.Text'{}.
-export_type(['Value.Text'/0]).

%% External functions.

%% @equiv get_env_value(Name, undefined)
-spec get_env_value(atom()) -> term().
get_env_value(Name) ->
    get_env_value(Name, undefined).

%% @doc Returns env value.
-spec get_env_value(atom(), term()) -> term().
get_env_value(Name, DefaultValue) ->
    case os:getenv("MESOS_" ++ string:to_upper(atom_to_list(Name))) of
        false ->
            DefaultValue;
        Value ->
            Value
    end.

%% @equiv get_env_converted_value(Type, Name, undefined)
-spec get_env_converted_value(atom(), atom()) -> term().
get_env_converted_value(Type, Name) ->
    get_env_converted_value(Type, Name, undefined).

%% @doc Returns env converted value.
-spec get_env_converted_value(atom(), atom(), term()) -> term().
get_env_converted_value(atom, Name, DefaultValue) ->
    case get_env_value(Name) of
        undefined ->
            DefaultValue;
        Value ->
            list_to_atom(Value)
    end;
get_env_converted_value(integer, Name, DefaultValue) ->
    case get_env_value(Name) of
        undefined ->
            DefaultValue;
        Value ->
            list_to_integer(Value)
    end;
get_env_converted_value(float, Name, DefaultValue) ->
    case get_env_value(Name) of
        undefined ->
            DefaultValue;
        Value ->
            list_to_float(Value)
    end;
get_env_converted_value(string, Name, DefaultValue) ->
    case get_env_value(Name) of
        undefined ->
            DefaultValue;
        Value ->
            Value
    end;
get_env_converted_value(binary, Name, DefaultValue) ->
    case get_env_value(Name) of
        undefined ->
            DefaultValue;
        Value ->
            list_to_binary(Value)
    end;
get_env_converted_value(interval, Name, DefaultValue) ->
    case get_env_value(Name) of
        undefined ->
            DefaultValue;
        Value ->
            convert_interval(Value)
    end.

%% Internal functions.

%% @doc Convert interval to milli seconds.
%% @private
-spec convert_interval(string()) -> float().
convert_interval(IntervalValue) ->
    convert_interval(IntervalValue, [], false).

%% @doc Convert interval to milli seconds.
%% @private
-spec convert_interval(string(), string(), boolean()) -> float().
convert_interval([$. = Char | Chars], IntervalChars, false) ->
    convert_interval(Chars, [Char | IntervalChars], true);
convert_interval([Char | Chars], IntervalChars, Dot)
    when Char >= $0, Char =< $9 ->
    convert_interval(Chars, [Char | IntervalChars], Dot);
convert_interval(UnitChars, IntervalChars, true) ->
    IntervalValue = list_to_float(lists:reverse(IntervalChars)),
    convert_interval(UnitChars, IntervalValue);
convert_interval(UnitChars, IntervalChars, false) ->
    IntervalValue = float(list_to_integer(lists:reverse(IntervalChars))),
    convert_interval(UnitChars, IntervalValue).

%% @doc Convert interval with unit to milli seconds.
%% @private
-spec convert_interval(string(), float()) -> float().
convert_interval("ns", IntervalValue) ->
    IntervalValue / 1000000;
convert_interval("us", IntervalValue) ->
    IntervalValue / 1000;
convert_interval("ms", IntervalValue) ->
    IntervalValue;
convert_interval("secs", IntervalValue) ->
    IntervalValue * 1000;
convert_interval("mins", IntervalValue) ->
    IntervalValue * 60000;
convert_interval("hrs", IntervalValue) ->
    IntervalValue * 3600000;
convert_interval("days", IntervalValue) ->
    IntervalValue * 86400000;
convert_interval("weeks", IntervalValue) ->
    IntervalValue * 604800000.

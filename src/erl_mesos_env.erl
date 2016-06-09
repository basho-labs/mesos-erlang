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

-module(erl_mesos_env).

-export([get_value/1,
         get_value/2,
         get_converted_value/2,
         get_converted_value/3]).

%% External functions.

%% @equiv get_value(Name, undefined)
-spec get_value(atom()) -> term().
get_value(Name) ->
    get_value(Name, undefined).

%% @doc Returns value.
-spec get_value(atom(), term()) -> term().
get_value(Name, DefaultValue) ->
    case os:getenv("MESOS_" ++ string:to_upper(atom_to_list(Name))) of
        false ->
            DefaultValue;
        Value ->
            Value
    end.

%% @equiv get_converted_value(Type, Name, undefined)
-spec get_converted_value(atom(), atom()) -> term().
get_converted_value(Type, Name) ->
    get_converted_value(Type, Name, undefined).

%% @doc Returns converted value.
-spec get_converted_value(atom(), atom(), term()) -> term().
get_converted_value(atom, Name, DefaultValue) ->
    case get_value(Name) of
        undefined ->
            DefaultValue;
        Value ->
            list_to_atom(Value)
    end;
get_converted_value(integer, Name, DefaultValue) ->
    case get_value(Name) of
        undefined ->
            DefaultValue;
        Value ->
            list_to_integer(Value)
    end;
get_converted_value(float, Name, DefaultValue) ->
    case get_value(Name) of
        undefined ->
            DefaultValue;
        Value ->
            list_to_float(Value)
    end;
get_converted_value(string, Name, DefaultValue) ->
    case get_value(Name) of
        undefined ->
            DefaultValue;
        Value ->
            Value
    end;
get_converted_value(binary, Name, DefaultValue) ->
    case get_value(Name) of
        undefined ->
            DefaultValue;
        Value ->
            list_to_binary(Value)
    end;
get_converted_value(interval, Name, DefaultValue) ->
    case get_value(Name) of
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

-module(erl_mesos_options).

-export([get_value/2, get_value/3]).

-export([delete/2]).

-type options() :: [{atom(), term()}].
-export_type([options/0]).

%% External functions.

%% @equiv get_value(atom(), options(), undefined)
-spec get_value(atom(), options()) -> term().
get_value(Name, Options) ->
    proplists:get_value(Name, Options, undefined).

%% @doc Returns option value.
-spec get_value(atom(), options(), term()) -> term().
get_value(Name, Options, DefaultValue) ->
    proplists:get_value(Name, Options, DefaultValue).

%% @doc Delete options.
-spec delete([atom()], options()) -> options().
delete([Key | Keys], Options) ->
    delete(Keys, proplists:delete(Key, Options));
delete([], Options) ->
    Options.

%% Internal functions.

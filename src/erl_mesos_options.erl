-module(erl_mesos_options).

-export([get_value/2, get_value/3]).

-export([delete/2]).

%% External functions.

get_value(Name, Options) ->
    proplists:get_value(Name, Options, undefined).

get_value(Name, Options, DefaultValue) ->
    proplists:get_value(Name, Options, DefaultValue).

delete([Key | Keys], Options) ->
    delete(Keys, proplists:delete(Key, Options));
delete([], Options) ->
    Options.

%% Internal functions.

%% %% @doc Sets default options.
%% -spec set_default_options([{atom(), term()}], [term()]) -> [{atom(), term()}].
%% set_default_options([{DefaultKey, _DefaultValue} = DefaultOption |
%%     DefaultOptions], Options) ->
%%     case lists:keyfind(DefaultKey, 1, Options) of
%%         {DefaultKey, _Value} ->
%%             set_default_options(DefaultOptions, Options);
%%         false ->
%%             set_default_options(DefaultOptions, [DefaultOption | Options])
%%     end;
%% set_default_options([], Options) ->
%%     Options.

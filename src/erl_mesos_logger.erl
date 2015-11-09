-module(erl_mesos_logger).

-export([info/2, warning/2, error/2]).

%% @doc Logs info.
-spec info(string(), list()) -> ok.
info(Format, Data) ->
    error_logger:info_msg(Format, Data).

%% @doc Logs warning.
-spec warning(string(), list()) -> ok.
warning(Format, Data) ->
    error_logger:warning_msg(Format, Data).

%% @doc Logs error.
-spec error(string(), list()) -> ok.
error(Format, Data) ->
    error_logger:error_msg(Format, Data).

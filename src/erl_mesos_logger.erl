-module(erl_mesos_logger).

-export([info/2, warning/2, error/2]).

info(Format, Data) ->
    error_logger:info_msg(Format, Data).

warning(Format, Data) ->
    error_logger:warning_msg(Format, Data).

error(Format, Data) ->
    error_logger:error_msg(Format, Data).

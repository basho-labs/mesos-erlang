-module(erl_mesos_http).

-export([request/5, body/1]).

-type headers() :: [{binary(), binary()}].
-export_type([headers/0]).

-type options() :: [{atom(), term()}].
-export_type([options/0]).

-type client_ref() :: hackney:client_ref().
-export_type([client_ref/0]).

%% External functions.

%% @doc Sends http request.
-spec request(atom(), binary(), headers(), binary(), options()) ->
    {ok, client_ref()} | {ok, non_neg_integer(), headers(), client_ref()} |
    {error, term()}.
request(Method, Url, Headers, Body, Options) ->
    hackney:request(Method, Url, Headers, Body, Options).

%% @doc Receives http request body.
-spec body(client_ref()) -> {ok, binary()} | {error, term()}.
body(ResponseRef) ->
    hackney:body(ResponseRef).

%% Internal functions.

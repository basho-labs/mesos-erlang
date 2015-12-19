-module(erl_mesos_http).

-export([request/5, body/1]).

-export([async_response/1, close_async_response/1]).

-type headers() :: [{binary(), binary()}].
-export_type([headers/0]).

-type options() :: [{atom(), term()}].
-export_type([options/0]).

-type client_ref() :: hackney:client_ref().
-export_type([client_ref/0]).

-type async_response() :: {status, non_neg_integer(), binary()} |
                          {headers, headers()} |
                          binary() |
                          done |
                          {error, term()}.

%% External functions.

%% @doc Sends http request.
-spec request(atom(), binary(), headers(), binary(), options()) ->
    {ok, client_ref()} | {ok, non_neg_integer(), headers(), client_ref()} |
    {error, term()}.
request(Method, Url, Headers, Body, Options) ->
    hackney:request(Method, Url, Headers, Body, Options).

%% @doc Receives http request body.
-spec body(client_ref()) -> {ok, binary()} | {error, term()}.
body(ClientRef) ->
    hackney:body(ClientRef).

%% @doc Returns async response.
-spec async_response({hackney_response, client_ref(), async_response()} |
                     term()) ->
    {response, client_ref(), async_response()} | undefined.
async_response({hackney_response, ClientRef, Response})
  when is_reference(ClientRef) ->
    {response, ClientRef, Response};
async_response(_Info) ->
    undefined.

%% @doc Closes async response.
-spec close_async_response(client_ref()) -> ok | {error, term()}.
close_async_response(ClientRef) ->
    hackney:close(ClientRef).

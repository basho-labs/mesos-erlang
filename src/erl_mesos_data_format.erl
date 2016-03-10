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

%% @private

-module(erl_mesos_data_format).

-export([content_type/1]).

-export([encode/2, decode/3]).

-export([decode_events/2]).

-type data_format() :: protobuf.
-export_type([data_format/0]).

-type message() :: tuple().
-export_type([message/0]).

%% External functions.

%% @doc Returns content type.
-spec content_type(data_format()) -> binary().
content_type(protobuf) ->
    <<"application/x-protobuf">>.

%% @doc Encodes message.
-spec encode(data_format(), message()) -> iodata().
encode(protobuf, Message) ->
    scheduler_protobuf:encode_msg(Message).

%% @doc Decodes binary.
-spec decode(data_format(), binary(), atom()) -> message().
decode(protobuf, Binary, MessageName) ->
    scheduler_protobuf:decode_msg(Binary, MessageName).

%% @doc Decodes events.
-spec decode_events(data_format(), binary()) -> [message()].
decode_events(protobuf, Binary) ->
    decode_protobuf_events(Binary, <<>>, []).

%% Internal functions.

%% @doc Decodes json events.
%% @private
-spec decode_protobuf_events(binary(), binary(), [message()]) -> [message()].
decode_protobuf_events(<<$\n, Chars/binary>>, SizeChars, Messages) ->
    Size = binary_to_integer(SizeChars),
    case Chars of
        <<Message:Size/binary>> ->
            lists:reverse([decode(protobuf, Message, 'Event') | Messages]);
        <<Message:Size/binary, RestChars/binary>> ->
            decode_protobuf_events(RestChars, <<>>,
                                   [decode(protobuf, Message, 'Event') |
                                    Messages])
    end;
decode_protobuf_events(<<Char, Chars/binary>>, SizeChars, Messages) ->
    decode_protobuf_events(Chars, <<SizeChars/binary, Char>>, Messages);
decode_protobuf_events(<<>>, _SizeChars, Messages) ->
    Messages.

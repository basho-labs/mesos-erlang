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

%% @private

-module(erl_mesos_data_format).

-export([content_type/1]).

-export([encode/2, decode/2]).

-export([decode_events/2]).

-type data_format() :: json.
-export_type([data_format/0]).

%% External functions.

%% @doc Returns content type.
-spec content_type(json) -> binary().
content_type(json) ->
    <<"application/json">>.

%% @doc Encodes data.
-spec encode(json, erl_mesos_obj:data()) -> iodata().
encode(json, Data) ->
    mochijson2:encode(Data).

%% @doc Decodes binary.
-spec decode(json, binary()) -> erl_mesos_obj:data().
decode(json, Binary) ->
    mochijson2:decode(Binary).

%% @doc Decodes events.
-spec decode_events(json, binary()) -> [erl_mesos_obj:data()].
decode_events(json, Binary) ->
    decode_json_events(Binary, <<>>, []).

%% Internal functions.

%% @doc Decodes json events.
%% @private
-spec decode_json_events(binary(), binary(), [erl_mesos_obj:data_obj()]) ->
    [erl_mesos_obj:data_obj()].
decode_json_events(<<$\n, Chars/binary>>, SizeChars, Packets) ->
    Size = binary_to_integer(SizeChars),
    case Chars of
        <<Packet:Size/binary>> ->
            lists:reverse([decode(json, Packet) | Packets]);
        <<Packet:Size/binary, RestChars/binary>> ->
            decode_json_events(RestChars, <<>>,
                               [decode(json, Packet) | Packets])
    end;
decode_json_events(<<Char, Chars/binary>>, SizeChars, Packets) ->
    decode_json_events(Chars, <<SizeChars/binary, Char>>, Packets);
decode_json_events(<<>>, _SizeChars, Packets) ->
    Packets.

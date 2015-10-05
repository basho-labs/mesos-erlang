-module(erl_mesos_data_format).

-export([content_type/1]).

-export([encode/2, decode/2]).

-export([decode_packets/2]).

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

%% @doc Decodes packets.
-spec decode_packets(json, binary()) -> [erl_mesos_obj:data()].
decode_packets(json, Binary) ->
    decode_json_packets(Binary, <<>>, []).

%% Internal functions.

%% @doc Decodes json packets.
%% @private
-spec decode_json_packets(binary(), binary(), [erl_mesos_obj:data_obj()]) ->
    [erl_mesos_obj:data_obj()].
decode_json_packets(<<$\n, Chars/binary>>, SizeChars, Packets) ->
    Size = binary_to_integer(SizeChars),
    case Chars of
        <<Packet:Size/binary>> ->
            lists:reverse([decode(json, Packet) | Packets]);
        <<Packet:Size/binary, RestChars/binary>> ->
            decode_json_packets(RestChars, <<>>,
                                [decode(json, Packet) | Packets])
    end;
decode_json_packets(<<Char, Chars/binary>>, SizeChars, Packets) ->
    decode_json_packets(Chars, <<SizeChars/binary, Char>>, Packets);
decode_json_packets(<<>>, _SizeChars, Packets) ->
    Packets.

-module(erl_mesos_obj).

-include("erl_mesos_obj.hrl").

-export([new/1]).

-export([get_value/2, get_value/3]).

-export([from_record/3, to_record/3]).

-type data_null() :: null.
-export_type([data_null/0]).

-type data_boolean() :: boolean().
-export_type([data_boolean/0]).

-type data_number() :: integer() | float().
-export_type([data_number/0]).

-type data_string() :: binary().
-export_type([data_string/0]).

-type data_array() :: [data()].
-export_type([data_array/0]).

-type data_obj() :: {struct, [{data_string(), data()}]}.
-export_type([data_obj/0]).

-type data() :: data_null() | data_boolean() | data_number() | data_string() |
                data_array() | data_obj().
-export_type([data/0]).

%% External functions.

%% @doc Returns new obj.
-spec new([{data_string(), data()}]) -> data_obj().
new(Fields) ->
    {struct, Fields}.

%% @equiv get_value(Key, Obj, undefined).
-spec get_value(data_string(), data_obj()) -> undefined | data().
get_value(Key, Obj) ->
    get_value(Key, Obj, undefined).

%% @doc Returns value.
-spec get_value(data_string(), data_obj(), term()) -> term() | data().
get_value(Key, {struct, Fields}, DefaultValue) ->
    proplists:get_value(Key, Fields, DefaultValue).

%% @doc Converts record to obj.
-spec from_record(atom(), tuple(), [atom()]) -> data_obj().
from_record(_RecordName, Record, RecordFields) ->
    {struct, encode_record_fields(Record, 2, RecordFields, [])}.

%% @doc Converts obj to record.
-spec to_record(data_obj(), tuple(), [atom()]) -> tuple().
to_record({struct, Fields}, Record, RecordFields) ->
    list_to_tuple([element(1, Record) |
                   decode_record_fields(Fields, Record, 2, RecordFields, [])]).

%% Internal functions.

%% @doc Encodes record fields.
%% @private
-spec encode_record_fields(tuple(), pos_integer(), [atom()],
                           [{data_string(), data()}]) ->
    [{data_string(), data()}].
encode_record_fields(_Record, _Index, [], Fields) ->
    Fields;
encode_record_fields(Record, Index, [RecordField | RecordFields], Fields) ->
    case element(Index, Record) of
        undefined ->
            encode_record_fields(Record, Index + 1, RecordFields, Fields);
        Value ->
            Field = {atom_to_binary(RecordField, utf8), Value},
            encode_record_fields(Record, Index + 1, RecordFields,
                                 [Field | Fields])
    end.

%% @doc Decodes record fields.
%% @private
-spec decode_record_fields([{data_string(), data()}], tuple(), pos_integer(),
                           [atom()], [data()]) ->
    [data()].
decode_record_fields(_Fields, _Record, _Index, [], Values) ->
    lists:reverse(Values);
decode_record_fields(Fields, Record, Index, [RecordField | RecordFields],
                     Values) ->
    case lists:keyfind(atom_to_binary(RecordField, utf8), 1, Fields) of
        {_, Value} ->
            decode_record_fields(Fields, Record, Index + 1, RecordFields,
                                 [Value | Values]);
        false ->
            decode_record_fields(Fields, Record, Index + 1, RecordFields,
                                 [element(Index, Record) | Values])
    end.

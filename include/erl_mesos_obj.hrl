-define(ERL_MESOS_OBJ_FROM_RECORD(RecordName, Record),
    erl_mesos_obj:from_record(RecordName, Record,
                              record_info(fields, RecordName))).

-define(ERL_MESOS_OBJ_TO_RECORD(RecordName, Obj),
    erl_mesos_obj:to_record(Obj, #RecordName{},
                            record_info(fields, RecordName))).

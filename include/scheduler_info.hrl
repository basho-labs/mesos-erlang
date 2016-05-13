-record(scheduler_info, {data_format :: erl_mesos_data_format:data_format(),
                         api_version :: erl_mesos_scheduler_call:version(),
                         master_host :: binary(),
                         request_options :: erl_mesos_http:options(),
                         subscribed :: boolean(),
                         framework_id :: erl_mesos:'FrameworkID'(),
                         stream_id :: undefined | binary()}).

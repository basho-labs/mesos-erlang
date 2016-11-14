-record(executor_info, {data_format :: erl_mesos_data_format:data_format(),
                        data_format_module :: module(),
                        api_version :: erl_mesos_scheduler_call:version(),
                        request_options :: erl_mesos_http:options(),
                        agent_host :: binary(),
                        subscribed :: boolean(),
                        executor_id :: erl_mesos:'ExecutorID'(),
                        framework_id :: erl_mesos:'FrameworkID'()}).

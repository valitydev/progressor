# progressor

## Usage
 TODO

 ## Observability

 \# HELP progressor_notification_duration_ms Notification durations in millisecond
 progressor_notification_duration_ms_bucket{service="hellgate",namespace="default",notification_type="lifecycle_sink",le="10"} 9311
 progressor_notification_duration_ms_bucket{service="hellgate",namespace="default",notification_type="lifecycle_sink",le="50"} 9311
 progressor_notification_duration_ms_bucket{service="hellgate",namespace="default",notification_type="lifecycle_sink",le="150"} 9311
 progressor_notification_duration_ms_bucket{service="hellgate",namespace="default",notification_type="lifecycle_sink",le="300"} 9311
 progressor_notification_duration_ms_bucket{service="hellgate",namespace="default",notification_type="lifecycle_sink",le="500"} 9311
 progressor_notification_duration_ms_bucket{service="hellgate",namespace="default",notification_type="lifecycle_sink",le="1000"} 9311
 progressor_notification_duration_ms_bucket{service="hellgate",namespace="default",notification_type="lifecycle_sink",le="+Inf"} 9311
 progressor_notification_duration_ms_count{service="hellgate",namespace="default",notification_type="lifecycle_sink"} 9311
 progressor_notification_duration_ms_sum{service="hellgate",namespace="default",notification_type="lifecycle_sink"} 60
 progressor_notification_duration_ms_bucket{service="hellgate",namespace="default",notification_type="event_sink",le="10"} 9311
 progressor_notification_duration_ms_bucket{service="hellgate",namespace="default",notification_type="event_sink",le="50"} 9311
 progressor_notification_duration_ms_bucket{service="hellgate",namespace="default",notification_type="event_sink",le="150"} 9311
 progressor_notification_duration_ms_bucket{service="hellgate",namespace="default",notification_type="event_sink",le="300"} 9311
 progressor_notification_duration_ms_bucket{service="hellgate",namespace="default",notification_type="event_sink",le="500"} 9311
 progressor_notification_duration_ms_bucket{service="hellgate",namespace="default",notification_type="event_sink",le="1000"} 9311
 progressor_notification_duration_ms_bucket{service="hellgate",namespace="default",notification_type="event_sink",le="+Inf"} 9311
 progressor_notification_duration_ms_count{service="hellgate",namespace="default",notification_type="event_sink"} 9311
 progressor_notification_duration_ms_sum{service="hellgate",namespace="default",notification_type="event_sink"} 19
 \# TYPE progressor_task_completion_duration_ms histogram
 \# HELP progressor_task_completion_duration_ms Task completion durations in millisecond
 progressor_task_completion_duration_ms_bucket{service="hellgate",namespace="default",completion_type="complete_and_suspend",le="50"} 26
 progressor_task_completion_duration_ms_bucket{service="hellgate",namespace="default",completion_type="complete_and_suspend",le="150"} 32
 progressor_task_completion_duration_ms_bucket{service="hellgate",namespace="default",completion_type="complete_and_suspend",le="300"} 50
 progressor_task_completion_duration_ms_bucket{service="hellgate",namespace="default",completion_type="complete_and_suspend",le="500"} 50
 progressor_task_completion_duration_ms_bucket{service="hellgate",namespace="default",completion_type="complete_and_suspend",le="750"} 50
 progressor_task_completion_duration_ms_bucket{service="hellgate",namespace="default",completion_type="complete_and_suspend",le="1000"} 50
 progressor_task_completion_duration_ms_bucket{service="hellgate",namespace="default",completion_type="complete_and_suspend",le="+Inf"} 50
 progressor_task_completion_duration_ms_count{service="hellgate",namespace="default",completion_type="complete_and_suspend"} 50
 progressor_task_completion_duration_ms_sum{service="hellgate",namespace="default",completion_type="complete_and_suspend"} 5100
 progressor_task_completion_duration_ms_bucket{service="hellgate",namespace="default",completion_type="complete_and_continue",le="50"} 298
 progressor_task_completion_duration_ms_bucket{service="hellgate",namespace="default",completion_type="complete_and_continue",le="150"} 4596
 progressor_task_completion_duration_ms_bucket{service="hellgate",namespace="default",completion_type="complete_and_continue",le="300"} 8926
 progressor_task_completion_duration_ms_bucket{service="hellgate",namespace="default",completion_type="complete_and_continue",le="500"} 9258
 progressor_task_completion_duration_ms_bucket{service="hellgate",namespace="default",completion_type="complete_and_continue",le="750"} 9261
 progressor_task_completion_duration_ms_bucket{service="hellgate",namespace="default",completion_type="complete_and_continue",le="1000"} 9261
 progressor_task_completion_duration_ms_bucket{service="hellgate",namespace="default",completion_type="complete_and_continue",le="+Inf"} 9261
 progressor_task_completion_duration_ms_count{service="hellgate",namespace="default",completion_type="complete_and_continue"} 9261
 progressor_task_completion_duration_ms_sum{service="hellgate",namespace="default",completion_type="complete_and_continue"} 1473582
 \# TYPE progressor_process_removing_duration_ms histogram
 \# HELP progressor_process_removing_duration_ms Task completion durations in millisecond
 \# TYPE progressor_task_processing_duration_ms histogram
 \# HELP progressor_task_processing_duration_ms Task processing durations in millisecond
 progressor_task_processing_duration_ms_bucket{service="hellgate",namespace="default",task_type="timeout",le="50"} 9261
 progressor_task_processing_duration_ms_bucket{service="hellgate",namespace="default",task_type="timeout",le="150"} 9261
 progressor_task_processing_duration_ms_bucket{service="hellgate",namespace="default",task_type="timeout",le="300"} 9261
 progressor_task_processing_duration_ms_bucket{service="hellgate",namespace="default",task_type="timeout",le="500"} 9261
 progressor_task_processing_duration_ms_bucket{service="hellgate",namespace="default",task_type="timeout",le="750"} 9261
 progressor_task_processing_duration_ms_bucket{service="hellgate",namespace="default",task_type="timeout",le="1000"} 9261
 progressor_task_processing_duration_ms_bucket{service="hellgate",namespace="default",task_type="timeout",le="+Inf"} 9261
 progressor_task_processing_duration_ms_count{service="hellgate",namespace="default",task_type="timeout"} 9261
 progressor_task_processing_duration_ms_sum{service="hellgate",namespace="default",task_type="timeout"} 176
 progressor_task_processing_duration_ms_bucket{service="hellgate",namespace="default",task_type="init",le="50"} 25
 progressor_task_processing_duration_ms_bucket{service="hellgate",namespace="default",task_type="init",le="150"} 25
 progressor_task_processing_duration_ms_bucket{service="hellgate",namespace="default",task_type="init",le="300"} 25
 progressor_task_processing_duration_ms_bucket{service="hellgate",namespace="default",task_type="init",le="500"} 25
 progressor_task_processing_duration_ms_bucket{service="hellgate",namespace="default",task_type="init",le="750"} 25
 progressor_task_processing_duration_ms_bucket{service="hellgate",namespace="default",task_type="init",le="1000"} 25
 progressor_task_processing_duration_ms_bucket{service="hellgate",namespace="default",task_type="init",le="+Inf"} 25
 progressor_task_processing_duration_ms_count{service="hellgate",namespace="default",task_type="init"} 25
 progressor_task_processing_duration_ms_sum{service="hellgate",namespace="default",task_type="init"} 0
 progressor_task_processing_duration_ms_bucket{service="hellgate",namespace="default",task_type="call",le="50"} 25
 progressor_task_processing_duration_ms_bucket{service="hellgate",namespace="default",task_type="call",le="150"} 25
 progressor_task_processing_duration_ms_bucket{service="hellgate",namespace="default",task_type="call",le="300"} 25
 progressor_task_processing_duration_ms_bucket{service="hellgate",namespace="default",task_type="call",le="500"} 25
 progressor_task_processing_duration_ms_bucket{service="hellgate",namespace="default",task_type="call",le="750"} 25
 progressor_task_processing_duration_ms_bucket{service="hellgate",namespace="default",task_type="call",le="1000"} 25
 progressor_task_processing_duration_ms_bucket{service="hellgate",namespace="default",task_type="call",le="+Inf"} 25
 progressor_task_processing_duration_ms_count{service="hellgate",namespace="default",task_type="call"} 25
 progressor_task_processing_duration_ms_sum{service="hellgate",namespace="default",task_type="call"} 1

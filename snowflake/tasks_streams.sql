-- snowflake/tasks_streams.sql
use database ACCT_POC;

-- Enable task graph (run once)
alter account set TASK_AUTO_ABORT_ON_ERROR = true;

-- Start tasks
alter task if exists CURATED.TASK_CDC_ACCOUNTS resume;
alter task if exists CURATED.TASK_CDC_JE resume;
alter task if exists CURATED.TASK_CDC_CLOSE resume;

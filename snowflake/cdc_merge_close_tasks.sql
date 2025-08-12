-- snowflake/cdc_merge_close_tasks.sql
use database ACCT_POC;

create or replace stream RAW.CLOSE_TASKS_RAW_STRM on table RAW.CLOSE_TASKS_RAW;

create or replace task CURATED.TASK_CDC_CLOSE
  warehouse = ETL_WH
  schedule = 'USING CRON */5 * * * * UTC'
as
merge into CURATED.CLOSE_TASKS t
using (
  select
    v:task_id::string     as task_id,
    v:name::string        as name,
    v:assigned_to::string as assigned_to,
    v:status::string      as status,
    to_date(v:due_date)   as due_date
  from RAW.CLOSE_TASKS_RAW_STRM
) s
on t.task_id = s.task_id
when matched then update set name=s.name, assigned_to=s.assigned_to, status=s.status, due_date=s.due_date
when not matched then insert (task_id, name, assigned_to, status, due_date)
values (s.task_id, s.name, s.assigned_to, s.status, s.due_date);

-- snowflake/raw_tables.sql
use database ACCT_POC;

create schema if not exists RAW;

-- RAW VARIANT landing (generic for ad hoc loads / Snowpipe target)
create or replace table RAW.ACCOUNTS_RAW (v variant);
create or replace table RAW.JE_RAW (v variant);
create or replace table RAW.CLOSE_TASKS_RAW (v variant);

-- Quick load from stage (array JSON supported via file format)
truncate table RAW.ACCOUNTS_RAW;
insert into RAW.ACCOUNTS_RAW
  select $1 as v from @RAW.RAW_STAGE (pattern=>'accounts\.json');

truncate table RAW.JE_RAW;
insert into RAW.JE_RAW
  select $1 as v from @RAW.RAW_STAGE (pattern=>'journal_entries\.json');

truncate table RAW.CLOSE_TASKS_RAW;
insert into RAW.CLOSE_TASKS_RAW
  select $1 as v from @RAW.RAW_STAGE (pattern=>'close_tasks\.json');

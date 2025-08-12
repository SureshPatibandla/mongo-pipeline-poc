-- snowflake/cdc_merge_accounts.sql
-- Example CDC using Streams + Task for ACCOUNTS
use database ACCT_POC;

create or replace stream RAW.ACCOUNTS_RAW_STRM on table RAW.ACCOUNTS_RAW;

create or replace task CURATED.TASK_CDC_ACCOUNTS
  warehouse = ETL_WH
  schedule = 'USING CRON */5 * * * * UTC'
as
merge into CURATED.ACCOUNTS t
using (
  select
    v:account_id::string as account_id,
    v:name::string       as name,
    v:type::string       as type
  from RAW.ACCOUNTS_RAW_STRM
) s
on t.account_id = s.account_id
when matched then update set name = s.name, type = s.type
when not matched then insert (account_id, name, type) values (s.account_id, s.name, s.type);

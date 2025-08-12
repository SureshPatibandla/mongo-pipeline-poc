-- snowflake/accounts_parse.sql
use database ACCT_POC;

create schema if not exists CURATED;

-- Target normalized table
create or replace table CURATED.ACCOUNTS (
  account_id string,
  name string,
  type string
);

-- Parse from RAW VARIANT to normalized
merge into CURATED.ACCOUNTS t
using (
  select
    v:account_id::string as account_id,
    v:name::string       as name,
    v:type::string       as type
  from RAW.ACCOUNTS_RAW
) s
on t.account_id = s.account_id
when matched then update set
  name = s.name,
  type = s.type
when not matched then insert (account_id, name, type)
values (s.account_id, s.name, s.type);

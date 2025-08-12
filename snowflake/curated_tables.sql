-- snowflake/curated_tables.sql
use database ACCT_POC;

create schema if not exists CURATED;

-- Hybrid flattened table: join line-level with account attributes
create or replace table CURATED.HYBRID_JE_ACCOUNTS as
select
  l.entry_id, l.date, l.year, l.month,
  l.account_id, a.name as account_name, a.type as account_type,
  l.debit, l.credit, j.total_debit, j.total_credit, j.is_balanced,
  l.description
from CURATED.JOURNAL_ENTRY_LINES l
left join CURATED.ACCOUNTS a on a.account_id = l.account_id
left join CURATED.JOURNAL_ENTRIES_AGG j on j.entry_id = l.entry_id and j.year = l.year and j.month = l.month;

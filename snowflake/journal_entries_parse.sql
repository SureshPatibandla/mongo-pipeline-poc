-- snowflake/journal_entries_parse.sql
use database ACCT_POC;

create schema if not exists CURATED;

-- Flattened lines table (one row per line)
create or replace table CURATED.JOURNAL_ENTRY_LINES (
  entry_id string,
  date date,
  account_id string,
  debit number(18,2),
  credit number(18,2),
  description string,
  year int,
  month int
);

-- Aggregate table per entry (totals + balance flag)
create or replace table CURATED.JOURNAL_ENTRIES_AGG (
  entry_id string,
  year int,
  month int,
  total_debit number(18,2),
  total_credit number(18,2),
  is_balanced boolean
);

-- Parse & upsert line-level
merge into CURATED.JOURNAL_ENTRY_LINES t
using (
  select
    v:entry_id::string as entry_id,
    to_date(v:date)    as date,
    l.value:account_id::string as account_id,
    l.value:debit::number(18,2)  as debit,
    l.value:credit::number(18,2) as credit,
    v:description::string as description,
    year(to_date(v:date))  as year,
    month(to_date(v:date)) as month
  from RAW.JE_RAW, lateral flatten(input => v:lines) l
) s
on t.entry_id = s.entry_id
   and t.account_id = s.account_id
   and t.year = s.year
   and t.month = s.month
when matched then update set
  date = s.date,
  debit = s.debit,
  credit = s.credit,
  description = s.description
when not matched then insert (entry_id, date, account_id, debit, credit, description, year, month)
values (s.entry_id, s.date, s.account_id, s.debit, s.credit, s.description, s.year, s.month);

-- Recompute totals
merge into CURATED.JOURNAL_ENTRIES_AGG t
using (
  select
    entry_id, year, month,
    sum(debit) as total_debit,
    sum(credit) as total_credit,
    iff(sum(debit)=sum(credit), true, false) as is_balanced
  from CURATED.JOURNAL_ENTRY_LINES
  group by entry_id, year, month
) s
on t.entry_id = s.entry_id and t.year = s.year and t.month = s.month
when matched then update set
  total_debit = s.total_debit,
  total_credit = s.total_credit,
  is_balanced = s.is_balanced
when not matched then insert (entry_id, year, month, total_debit, total_credit, is_balanced)
values (s.entry_id, s.year, s.month, s.total_debit, s.total_credit, s.is_balanced);

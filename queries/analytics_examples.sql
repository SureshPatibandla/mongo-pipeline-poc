-- queries/analytics_examples.sql

-- Net change by account for May 2025
select account_id, sum(debit - credit) as net_change
from CURATED.JOURNAL_ENTRY_LINES
where year = 2025 and month = 5
group by account_id
order by account_id;

-- Unbalanced entries (should be zero)
select entry_id, year, month, total_debit, total_credit
from CURATED.JOURNAL_ENTRIES_AGG
where is_balanced = false;

-- Top 10 accounts by activity
select account_id, count(*) as lines, sum(abs(debit - credit)) as magnitude
from CURATED.JOURNAL_ENTRY_LINES
group by account_id
order by magnitude desc
limit 10;

-- Hybrid view example
select * from CURATED.HYBRID_JE_ACCOUNTS order by date desc limit 50;

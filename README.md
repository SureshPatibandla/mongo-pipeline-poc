# mongo-pipeline-poc
This is a sample pipeline to be used for outlining the process to load data from Mongo to a cloud instance and then parse it for aggregations

# Mongo → Snowflake / Iceberg POC

This repo supports a sample pipeline for loading **MongoDB** data (some simple and nested collections) and transforming it in **Snowflake** (and optionally Databricks).

## Structure
- 'snowflake/' — SQL to create stages, raw tables, curated tables; **separated JSON parsers**; and CDC merges.
- 'batch/' — Python to connect to **MongoDB Atlas** and export JSON files for staging.
- 'databricks/' — Example Spark code to parse the same datasets and (optionally) write to Snowflake.
- 'queries/' — Example analytical SQL queries.

## Run Order (Snowflake)
1. Edit and run 'snowflake/stages.sql' (stage + file format).
2. Upload files to the stage (or use Snowpipe; see comments).
3. Run 'snowflake/raw_tables.sql' to create RAW VARIANT tables.
4. Run the **parsers** (separated):
   - 'snowflake/accounts_parse.sql'
   - 'snowflake/journal_entries_parse.sql'
   - 'snowflake/close_tasks_parse.sql'
5. Create curated targets: 'snowflake/curated_tables.sql'
6. Run CDC merges:
   - 'snowflake/cdc_merge_accounts.sql'
   - 'snowflake/cdc_merge_journal_entries.sql'
   - 'snowflake/cdc_merge_close_tasks.sql'
7. Optional: set up Streams & Tasks: 'snowflake/tasks_streams.sql'
8. Explore analytics: 'queries/analytics_examples.sql'

> Namespaces are 'RAW' and 'CURATED' by default. can change as needed.


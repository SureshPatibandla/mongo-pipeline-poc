-- snowflake/stages.sql
-- Adjust storage integration / credentials as appropriate.

create or replace database if not exists ACCT_POC;
use database ACCT_POC;

create schema if not exists RAW;
create schema if not exists CURATED;

-- JSON file format
create or replace file format UTIL.JSON_FF type = JSON strip_outer_array = true;

-- Internal stage for quick demo
create or replace stage RAW.RAW_STAGE file_format = UTIL.JSON_FF;

-- Example PUT (run from SnowSQL client; path is local to your machine)
-- PUT file://./accounts.json @RAW.RAW_STAGE auto_compress=false overwrite=true;
-- PUT file://./journal_entries.json @RAW.RAW_STAGE auto_compress=false overwrite=true;
-- PUT file://./close_tasks.json @RAW.RAW_STAGE auto_compress=false overwrite=true;

-- Or use Snowpipe (auto-ingest) if sourcing from S3/Azure Blob; see Snowflake docs.

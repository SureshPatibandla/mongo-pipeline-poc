# batch/transform_load.py
# Optional helper to push exported JSONs into Snowflake via PUT/COPY:
# 1) Use SnowSQL to PUT the files to @RAW.RAW_STAGE
# 2) Run the SQL scripts in snowflake/*.sql

print("Use SnowSQL:")
print("  snowsql -q "PUT file://./accounts.json @ACCT_POC.RAW.RAW_STAGE auto_compress=false overwrite=true;"")
print("  snowsql -q "PUT file://./journal_entries.json @ACCT_POC.RAW.RAW_STAGE auto_compress=false overwrite=true;"")
print("  snowsql -q "PUT file://./close_tasks.json @ACCT_POC.RAW.RAW_STAGE auto_compress=false overwrite=true;"")

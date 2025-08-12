# databricks/parse_databricks.py
# Example Spark code (Databricks) reading the same JSONs and writing to Snowflake.
# Requires the Snowflake Spark connector & a Snowflake account with a warehouse.

from pyspark.sql import functions as F

# Load local JSONs (uploaded to DBFS or mounted)
accounts = spark.read.json("/FileStore/tables/accounts.json")
je = spark.read.json("/FileStore/tables/journal_entries.json")
close = spark.read.json("/FileStore/tables/close_tasks.json")

accounts_sel = accounts.select(
    F.col("account_id").cast("string"),
    F.col("name").cast("string"),
    F.col("type").cast("string")
)

je_lines = (je
    .withColumn("date", F.to_date("date"))
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("line", F.explode("lines"))
    .select(
        F.col("entry_id").cast("string").alias("entry_id"),
        F.col("date"),
        F.col("year"), F.col("month"),
        F.col("line.account_id").cast("string").alias("account_id"),
        F.col("line.debit").cast("double").alias("debit"),
        F.col("line.credit").cast("double").alias("credit"),
        F.col("description").cast("string").alias("description")
    ))

totals = (je_lines.groupBy("entry_id","year","month")
          .agg(F.sum("debit").alias("total_debit"), F.sum("credit").alias("total_credit")))

je_enriched = (je_lines.join(totals, ["entry_id","year","month"])
               .withColumn("is_balanced", F.col("total_debit") == F.col("total_credit")))

close_sel = close.select(
    F.col("task_id").cast("string"),
    F.col("name").cast("string"),
    F.col("assigned_to").cast("string"),
    F.col("status").cast("string"),
    F.to_date("due_date").alias("due_date")
)

# Example: write to Snowflake (placeholders)
sfOptions = {
  "sfURL": "<account>.snowflakecomputing.com",
  "sfUser": "<user>",
  "sfPassword": "<password>",
  "sfDatabase": "ACCT_POC",
  "sfSchema": "CURATED",
  "sfWarehouse": "ETL_WH",
  "sfRole": "SYSADMIN"
}

accounts_sel.write.format("snowflake").options(**sfOptions).option("dbtable","ACCOUNTS").mode("overwrite").save()
je_enriched.write.format("snowflake").options(**sfOptions).option("dbtable","JOURNAL_ENTRY_LINES").mode("overwrite").save()
close_sel.write.format("snowflake").options(**sfOptions).option("dbtable","CLOSE_TASKS").mode("overwrite").save()

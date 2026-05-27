# Pipeline Overview

This pipeline ingests transaction data, transforms it, and loads it into bronze, silver, and gold tables. It runs to provide up-to-date transaction data for reporting and analysis. If this pipeline fails, downstream reports and dashboards will be inaccurate or missing data.

## Pipeline Steps

1. Connect to the DuckDB database using `get_connection()`.
2. Set up necessary tables using `setup_tables()`.
3. Load merchant data into the `merchants` table using `load_merchants()`.
4. Load all transactions into the `bronze_transactions` table using `load_bronze()`.
5. Transform bronze transactions to silver using `transform_bronze_to_silver()`.
6. Load transformed transactions into the `silver_transactions` table using `load_silver()`.
7. Compute merchant performance metrics using `compute_merchant_performance()`.
8. Compute daily summary metrics using `compute_daily_summary()`.
9. Load merchant performance and daily summary into gold tables using `load_gold()`.

## Schedule / Trigger

This pipeline runs every night at 2 AM UTC via a cron job.

## Failure Modes

1. **Database Connection Failure**
   - **Root Cause:** DuckDB service is down.
   - **Symptom:** `get_connection()` raises an exception.
2. **Table Creation Failure**
   - **Root Cause:** Syntax error in SQL.
   - **Symptom:** `setup_tables()` raises an exception.
3. **Merchant Data Load Failure**
   - **Root Cause:** Corrupt merchant data.
   - **Symptom:** `load_merchants()` raises an exception.
4. **Bronze Load Failure**
   - **Root Cause:** Malformed transaction data.
   - **Symptom:** `load_bronze()` raises an exception.
5. **Silver Transformation Failure**
   - **Root Cause:** Missing merchant ID in transactions.
   - **Symptom:** `transform_bronze_to_silver()` raises an exception.

## Recovery Actions

1. **Database Connection Failure**
   - Notify Platform Manager: `kavya.reddy@sigmadatatech.in`
   - Check DuckDB service status.
   - Restart DuckDB service if necessary.
2. **Table Creation Failure**
   - Review SQL in `setup_tables()`.
   - Fix syntax error.
   - Re-run pipeline.
3. **Merchant Data Load Failure**
   - Inspect `MERCHANTS` data for corruption.
   - Clean data and re-run `load_merchants()`.
4. **Bronze Load Failure**
   - Inspect `TRANSACTIONS_CLEAN` and `TRANSACTIONS_DIRTY` for malformed data.
   - Clean data and re-run `load_bronze()`.
5. **Silver Transformation Failure**
   - Ensure all transactions have a valid `merchant_id`.
   - Clean data and re-run `transform_bronze_to_silver()`.

## Known Bugs

- Hardcoded AWS credentials in the code.
- Lack of null handling in `transform_bronze_to_silver()`.

## Escalation Contacts

1. On-call DE: Priya Nair (`priya.nair@sigmadatatech.in`, +91-98400-11111)
2. Tech Lead: Arjun Mehta (`arjun.mehta@sigmadatatech.in`)
3. Platform Manager: Kavya Reddy (`kavya.reddy@sigmadatatech.in`)

## Data Quality Checks

- Verify the number of records in `bronze_transactions`, `silver_transactions`, `gold_merchant_performance`, and `gold_daily_summary`.
- Ensure `quality_flag` is set correctly in `silver_transactions`.
- Check for duplicate `transaction_id` in `silver_transactions`.
- Validate merchant performance and daily summary metrics against expected values.
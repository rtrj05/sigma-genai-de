# Data Pipeline Design Document

## What This Pipeline Does
This pipeline ingests transaction data, enriches it with merchant information, and then aggregates it into merchant performance metrics and daily summaries.

## Data Flow Diagram

```
+---------------------+       +--------------------+       +--------------------+       +--------------------+
|     Source          |       |     Bronze         |       |     Silver         |       |       Gold         |
|  TRANSACTIONS_CLEAN | --->  | bronze_transactions| --->  | silver_transactions | --->  | gold_merchant_perf  |
|  TRANSACTIONS_DIRTY |       |                    |       |                    |       | gold_daily_summary  |
+---------------------+       +--------------------+       +--------------------+       +--------------------+
```

## Key Design Decisions
- **Layered Data Processing:** The pipeline uses a three-layer approach (Bronze, Silver, Gold) to ensure data quality and enrichment before aggregation.
- **Data Quality Checks:** Negative amounts and duplicate transactions are filtered out in the Silver layer.
- **Merchant Enrichment:** Merchant details are joined with transaction data to enrich the dataset.
- **Aggregation:** Separate functions for merchant performance and daily summaries ensure modular and maintainable code.

## Known Limitations
- **Data Loss:** Transactions with missing merchant IDs are skipped in the Silver layer.
- **Performance:** The pipeline does not handle very large datasets efficiently due to in-memory transformations.
- **Static Merchant Data:** Merchants data is loaded only once; updates require pipeline restart.
- **Single-Day Summaries:** The Gold layer only aggregates data for the current day.

## Dependencies
- **DuckDB Database:** For storing and querying data.
- **MERCHANTS List:** A predefined list of merchant data.
- **TRANSACTIONS_CLEAN and TRANSACTIONS_DIRTY:** Source data files containing transaction records.
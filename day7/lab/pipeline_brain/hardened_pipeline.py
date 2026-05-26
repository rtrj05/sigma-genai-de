import logging
import shutil
from datetime import datetime
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import StringType, FloatType, DateType

logging.basicConfig(level=logging.INFO)

def ingest_bronze(spark, input_path, output_path, run_date, run_id):
    try:
        logging.info("Starting bronze ingestion")
        transactions_df = spark.read.csv(input_path, header=True, inferSchema=False)
        merchants_df = spark.read.csv(input_path.replace('transactions','merchants'), header=True, inferSchema=False)

        transactions_df = transactions_df.withColumn("ingestion_timestamp", F.lit(run_date))
        transactions_df = transactions_df.withColumn("source_file", F.lit("transactions.csv"))
        transactions_df = transactions_df.withColumn("pipeline_run_id", F.lit(run_id))

        partition_path = f"{output_path}/transactions/ingestion_timestamp={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        transactions_df.write.mode('overwrite').partitionBy("ingestion_timestamp").parquet(output_path + "/transactions")

        partition_path = f"{output_path}/merchants/ingestion_timestamp={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        merchants_df.write.mode('overwrite').partitionBy("ingestion_timestamp").parquet(output_path + "/merchants")

        logging.info(f"[Stage: Bronze Ingestion] Output: {transactions_df.count():,} rows")
    except Exception as e:
        logging.error(f"Error in bronze ingestion: {e}")
        raise

def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    try:
        logging.info("Starting silver transformation")
        transactions_df = (spark.read.parquet(bronze_path)
                          .filter(F.col("ingestion_timestamp") == run_date)
                           .cache())

        merchants_df = (spark.read.parquet(merchants_path)
                        .filter(F.col("ingestion_timestamp") == run_date)
                       .withColumnRenamed("_c0", "merchant_id")
                       .withColumnRenamed("_c1", "merchant_name")
                       .withColumnRenamed("_c2", "category")
                       .withColumnRenamed("_c3", "city"))

        transactions_df = transactions_df.withColumn("amount", F.col("amount").cast(FloatType()))
        transactions_df = transactions_df.withColumn("transaction_date", F.col("transaction_date").cast(DateType()))
        transactions_df = transactions_df.withColumn("transaction_id", F.col("transaction_id").cast(StringType()))
        transactions_df = transactions_df.withColumn("merchant_id", F.col("merchant_id").cast(StringType()))

        transactions_df = transactions_df.filter((F.col("transaction_id").isNotNull()) & (F.col("amount") >= 0))
        logging.info(f"[Stage: Silver Transformation] After filter: {transactions_df.count():,} rows")

        transactions_df = (transactions_df.withColumn("rank", F.row_number().over(Window.partitionBy("transaction_id").orderBy(F.col("ingestion_timestamp").desc_nulls_last())))
                          .filter(F.col("rank") == 1)
                           .drop("rank"))

        logging.info(f"[Stage: Silver Transformation] After deduplication: {transactions_df.count():,} rows")

        enriched_df = (transactions_df.join(F.broadcast(merchants_df), "merchant_id", "left")
                        .withColumn("quality_flag", F.when(F.col("merchant_id").isNull(), "UNMATCHED").otherwise("CLEAN")))

        partition_path = f"{output_path}/transaction_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        enriched_df.write.mode('overwrite').partitionBy("transaction_date").parquet(output_path)

        logging.info(f"[Stage: Silver Transformation] Output: {enriched_df.count():,} rows")
    except Exception as e:
        logging.error(f"Error in silver transformation: {e}")
        raise

def build_merchant_performance(spark, silver_path, output_path, run_date):
    try:
        logging.info("Starting merchant performance aggregation")
        silver_df = spark.read.parquet(silver_path).filter(F.col("transaction_date") == run_date)

        merchant_performance_df = silver_df.groupBy("merchant_id", "merchant_name", "category", "city", "transaction_date") \
           .agg(
                F.sum(F.when(F.col("status") == "COMPLETED", F.col("amount")).otherwise(0)).alias("total_revenue"),
                F.count("*").alias("txn_count"),
                (F.count(F.when(F.col("status") == "FAILED", 1)) / F.count("*") * 100).alias("failure_rate_pct")
            )

        partition_path = f"{output_path}/transaction_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        merchant_performance_df.write.mode("overwrite").partitionBy("transaction_date").parquet(output_path)

        logging.info(f"[Stage: Merchant Performance] Output: {merchant_performance_df.count():,} rows")
    except Exception as e:
        logging.error(f"Error in merchant performance aggregation: {e}")
        raise

def build_customer_ltv(spark, silver_path, output_path):
    try:
        logging.info("Starting customer LTV aggregation")
        silver_df = spark.read.parquet(silver_path)

        customer_ltv_df = silver_df.groupBy("customer_id") \
           .agg(
                F.sum(F.when(F.col("status") == "COMPLETED", F.col("amount")).otherwise(0)).alias("total_spent"),
                F.count("*").alias("total_txns"),
                F.avg(F.col("amount")).alias("avg_txn_value"),
                F.first("transaction_date").alias("first_txn_date"),
                F.last("transaction_date").alias("last_txn_date"),
                F.expr("percentile_approx(amount, 0.5)").alias("preferred_payment_method")
            )

        customer_ltv_df.write.mode("overwrite").parquet(output_path)

        logging.info(f"[Stage: Customer LTV] Output: {customer_ltv_df.count():,} rows")
    except Exception as e:
        logging.error(f"Error in customer LTV aggregation: {e}")
        raise

def build_daily_summary(spark, silver_path, output_path, run_date):
    try:
        logging.info("Starting daily summary aggregation")
        silver_df = spark.read.parquet(silver_path).filter(F.col("transaction_date") == run_date)

        daily_summary_df = silver_df.groupBy("transaction_date") \
           .agg(
                F.sum(F.when(F.col("status") == "COMPLETED", F.col("amount")).otherwise(0)).alias("total_revenue"),
                F.count("*").alias("total_txns"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.countDistinct("merchant_id").alias("unique_merchants"),
                (F.count(F.when(F.col("status") == "FAILED", 1)) / F.count("*") * 100).alias("failure_rate_pct")
            )

        partition_path = f"{output_path}/transaction_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        daily_summary_df.write.mode("overwrite").partitionBy("transaction_date").parquet(output_path)

        logging.info(f"[Stage: Daily Summary] Output: {daily_summary_df.count():,} rows")
    except Exception as e:
        logging.error(f"Error in daily summary aggregation: {e}")
        raise

def run_pipeline(spark, input_path, bronze_path, merchants_path, silver_path, gold_output_dir, run_date, run_id):
    try:
        started_at = datetime.now().isoformat()
        logging.info(f"Pipeline started at: {started_at}")

        ingest_bronze(spark, input_path, bronze_path, run_date, run_id)
        transform_silver(spark, bronze_path, merchants_path, silver_path, run_date)

        run_gold(spark, silver_path, gold_output_dir, run_date)

        completed_at = datetime.now().isoformat()
        run_status = "SUCCESS"
        error_message = None

        logging.info(f"Pipeline completed at: {completed_at}")

        run_metadata = {
            "pipeline_name": "Sigma DataTech Transaction Analytics Pipeline",
            "run_date": run_date,
            "run_id": run_id,
            "run_status": run_status,
            "error_message": error_message,
            "started_at": started_at,
            "completed_at": completed_at,
            "bronze_ingest_count": spark.read.parquet(bronze_path).count(),
            "silver_transform_count": spark.read.parquet(silver_path).count(),
            "gold_merchant_performance_count": spark.read.parquet(f"{gold_output_dir}/merchant_performance").count(),
            "gold_customer_ltv_count": spark.read.parquet(f"{gold_output_dir}/customer_ltv").count(),
            "gold_daily_summary_count": spark.read.parquet(f"{gold_output_dir}/daily_summary").count()
        }

        with open(f"{gold_output_dir}/run_metadata_{run_date}.json", "w") as f:
            json.dump(run_metadata, f)

    except Exception as e:
        completed_at = datetime.now().isoformat()
        run_status = "FAILED"
        error_message = str(e)

        logging.error(f"Pipeline failed at: {completed_at} with error: {error_message}")

        run_metadata = {
            "pipeline_name": "Sigma DataTech Transaction Analytics Pipeline",
            "run_date": run_date,
            "run_id": run_id,
            "run_status": run_status,
            "error_message": error_message,
            "started_at": started_at,
            "completed_at": completed_at
        }

        with open(f"{gold_output_dir}/run_metadata_{run_date}.json", "w") as f:
            json.dump(run_metadata, f)

        raise

def run_gold(spark, silver_path, gold_output_dir, run_date):
    try:
        logging.info("Starting gold layer aggregations")
        merchant_performance_output_path = f"{gold_output_dir}/merchant_performance"
        customer_ltv_output_path = f"{gold_output_dir}/customer_ltv"
        daily_summary_output_path = f"{gold_output_dir}/daily_summary"

        build_merchant_performance(spark, silver_path, merchant_performance_output_path, run_date)
        build_customer_ltv(spark, silver_path, customer_ltv_output_path)
        build_daily_summary(spark, silver_path, daily_summary_output_path, run_date)

        run_metadata = {
            "run_date": run_date,
            "silver_path": silver_path,
            "gold_output_dir": gold_output_dir,
            "tables": [
                {"name": "merchant_performance", "path": merchant_performance_output_path},
                {"name": "customer_ltv", "path": customer_ltv_output_path},
                {"name": "daily_summary", "path": daily_summary_output_path}
            ]
        }

        spark.sparkContext.parallelize([run_metadata]).write.json(f"{gold_output_dir}/run_metadata")
    except Exception as e:
        logging.error(f"Error in gold layer aggregations: {e}")
        raise

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Sigma DataTech Transaction Analytics Pipeline").getOrCreate()

    input_path = "s3://your-bucket/bronze/"
    bronze_path = "s3://your-bucket/silver/"
    merchants_path = "s3://your-bucket/bronze/merchants/"
    silver_path = "s3://your-bucket/gold/"
    gold_output_dir = "s3://your-bucket/gold/"
    run_date = "2026-05-27"
    run_id = "run_001"

    run_pipeline(spark, input_path, bronze_path, merchants_path, silver_path, gold_output_dir, run_date, run_id)

#!/usr/bin/env python3
import os
import sys
from pyspark.sql import SparkSession
from src.bronze import process_batch_bronze, process_streaming_bronze
from src.silver import process_silver
from src.gold import process_gold
from src.serving import serve_to_cassandra, execute_demo_queries, test_idempotency

def init_spark():
    print("Initializing Spark Session (Modular)...")
    spark = SparkSession.builder \
        .appName("CloudProviderAnalyticsPipelineModular") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.default.parallelism", "4") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def main():
    spark = init_spark()
    try:
        # 1 & 2. Ingestion to Bronze (Batch & Streaming)
        process_batch_bronze(spark)
        process_streaming_bronze(spark)
        
        # 3. Conformance, Enrichment, Quality Rules in Silver
        process_silver(spark)
        
        # 4. Aggregations in Gold
        gold_df = process_gold(spark)
        
        # 5. Serving load to Cassandra
        serve_to_cassandra(spark, gold_df)
        
        # 6. Execute Analytics Demo Queries
        execute_demo_queries()
        
        # 7. Validate Pipeline Idempotency
        test_idempotency(spark)
        
        print("\n=== MODULAR PIPELINE COMPLETED SUCCESSFULLY ===")
        
    finally:
        print("Stopping Spark Session...")
        spark.stop()

if __name__ == "__main__":
    main()

# Gold Layer FinOps Mart Aggregation Module
from pyspark.sql.functions import sum as _sum, round as _round
from src import config

def process_gold(spark):
    """
    Gold FinOps aggregation mart creation (daily by organization/service grain).
    """
    print("\n--- Phase 4: Gold FinOps Mart Creation ---")
    
    # Read Silver data
    silver_df = spark.read.parquet(f"{config.SILVER_DIR}/usage_events")
    
    # Aggregate daily metrics
    print("Aggregating daily usage metrics by organization and service...")
    gold_df = silver_df.groupBy("org_id", "usage_date", "service") \
        .agg(
            _round(_sum("daily_cost_usd"), 4).alias("total_cost_usd"),
            _sum("requests").alias("total_requests"),
            _round(_sum("cpu_hours"), 4).alias("total_cpu_hours"),
            _round(_sum("storage_gb_hours"), 4).alias("total_storage_gb_hours"),
            _sum("genai_tokens_feat").alias("total_genai_tokens"),
            _round(_sum("carbon_kg_feat"), 6).alias("total_carbon_kg")
        )
    
    # Write to Gold
    gold_df.write.mode("overwrite").parquet(f"{config.GOLD_DIR}/org_daily_usage_by_service")
    print(f"Gold FinOps Mart count: {gold_df.count()}")
    
    return gold_df

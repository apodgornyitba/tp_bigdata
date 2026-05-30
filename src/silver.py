# Silver Layer Conformance, Enrichment, and Quality Quarantine Module
from pyspark.sql.functions import col, coalesce, lit, when, to_date
from src import config

def process_silver(spark):
    """
    Silver conformances, joins with dimensions, feature calculation, 
    data quality checks, and quarantine filtering.
    """
    print("\n--- Phase 3: Silver Conformance, Enrichment, Features and Quality (Quarantine) ---")
    
    # 1. Read raw Bronze tables
    events_df = spark.read.parquet(f"{config.BRONZE_DIR}/usage_events")
    orgs_df = spark.read.parquet(f"{config.BRONZE_DIR}/customers_orgs")
    res_df = spark.read.parquet(f"{config.BRONZE_DIR}/resources")
    
    # 2. Enrichment Joins
    print("Enriching events with Organization and Resource dimension tables...")
    enriched_df = events_df \
        .join(orgs_df.select("org_id", "org_name", "plan_tier", "is_enterprise"), "org_id", "left") \
        .join(res_df.select("resource_id", "state"), "resource_id", "left")
        
    # 3. Feature Engineering & Conformance
    print("Calculating features...")
    # Calculate: requests, cpu_hours, storage_gb_hours, genai_tokens, carbon_kg
    features_df = enriched_df.withColumn(
        "requests",
        when((col("metric") == "requests") & (col("value").isNotNull()), col("value"))
        .when(col("metric") == "requests", lit(1.0))
        .otherwise(lit(0.0))
    ).withColumn(
        "cpu_hours",
        when((col("metric") == "cpu_hours") & (col("value").isNotNull()), col("value"))
        .otherwise(lit(0.0))
    ).withColumn(
        "storage_gb_hours",
        when((col("metric") == "storage_gb_hours") & (col("value").isNotNull()), col("value"))
        .otherwise(lit(0.0))
    ).withColumn(
        "genai_tokens_feat",
        when((col("service") == "genai") & (col("genai_tokens").isNotNull()), col("genai_tokens"))
        .otherwise(lit(0))
    ).withColumn(
        "carbon_kg_feat",
        coalesce(col("carbon_kg"), lit(0.0))
    ).withColumn(
        "daily_cost_usd",
        coalesce(col("cost_usd_increment"), lit(0.0))
    )

    # Anomaly spike detection
    features_df = features_df.withColumn(
        "anomaly_flag",
        when((col("daily_cost_usd") > 100.0) | (col("daily_cost_usd") < 0.0), lit(True)).otherwise(lit(False))
    )

    # 4. Data Quality Rules Check
    # Rule 1: event_id is not null
    rule_event_id_not_null = col("event_id").isNotNull()
    
    # Rule 2: cost_usd_increment is not negative (less than -0.01)
    rule_cost_valid = col("cost_usd_increment") >= -0.01
    
    # Rule 3: unit is not null when value exists
    rule_unit_not_null_when_val_exists = ~(col("value").isNotNull() & col("unit").isNull())
    
    # Composite logical check
    is_valid = rule_event_id_not_null & rule_cost_valid & rule_unit_not_null_when_val_exists
    
    # Separate valid events and invalid quarantined events
    valid_df = features_df.filter(is_valid)
    quarantine_df = features_df.filter(~is_valid)
    
    # 5. Write results
    print("Writing Silver conformed events...")
    valid_df = valid_df.withColumn("usage_date", to_date("timestamp_parsed"))
    valid_df.write.mode("overwrite") \
        .partitionBy("usage_date", "service") \
        .parquet(f"{config.SILVER_DIR}/usage_events")
    print(f"Silver count: {valid_df.count()}")
    
    print("Writing Quarantine events...")
    quarantine_df.write.mode("overwrite") \
        .parquet(f"{config.QUARANTINE_DIR}/usage_events")
    print(f"Quarantine count: {quarantine_df.count()}")

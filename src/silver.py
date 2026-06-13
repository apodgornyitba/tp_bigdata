# Silver Layer Conformance, Enrichment, and Quality Quarantine Module
from pyspark.sql.functions import col, coalesce, lit, when, to_date, expr
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

    # 4. Anomaly spike detection using IQR (percentiles / p-tiles)
    print("Calculating IQR statistical bounds for anomaly detection...")
    # Compute Q1 and Q3 dynamically per service
    percentiles_df = features_df.groupBy("service").agg(
        expr("percentile_approx(daily_cost_usd, 0.25)").alias("q1"),
        expr("percentile_approx(daily_cost_usd, 0.75)").alias("q3")
    )
    # Calculate IQR and bounds (preventing narrow/zero bounds by using a minimal offset of 10.0 if IQR is 0)
    bounds_df = percentiles_df.withColumn("iqr", col("q3") - col("q1")) \
                              .withColumn("upper_bound", when(col("iqr") == 0.0, col("q3") + 10.0).otherwise(col("q3") + 1.5 * col("iqr"))) \
                              .withColumn("lower_bound", when(col("iqr") == 0.0, col("q1") - 10.0).otherwise(col("q1") - 1.5 * col("iqr")))
    
    # Join bounds back to features_df
    features_df = features_df.join(bounds_df.select("service", "upper_bound", "lower_bound"), "service", "left")
    
    # Flag anomaly: if daily_cost_usd is outside [lower_bound, upper_bound]
    features_df = features_df.withColumn(
        "anomaly_flag",
        when((col("daily_cost_usd") > col("upper_bound")) | (col("daily_cost_usd") < col("lower_bound")), lit(True)).otherwise(lit(False))
    )

    # 5. Data Quality Rules Check
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
    
    # Write results
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

    # 6. Process Support Tickets in Silver
    print("Processing and cleaning support tickets in Silver...")
    tickets_bronze_df = spark.read.parquet(f"{config.BRONZE_DIR}/support_tickets")
    tickets_silver_df = tickets_bronze_df \
        .withColumn("csat", coalesce(col("csat"), lit(0.0))) \
        .withColumn("sla_breached", coalesce(col("sla_breached"), lit(False))) \
        .withColumn("ticket_date", col("created_at"))
        
    tickets_silver_df.write.mode("overwrite") \
        .partitionBy("ticket_date") \
        .parquet(f"{config.SILVER_DIR}/support_tickets")
    print(f"Silver Support Tickets count: {tickets_silver_df.count()}")
    
    # 7. Process Billing Monthly in Silver
    print("Processing and normalizing billing monthly in Silver...")
    billing_bronze_df = spark.read.parquet(f"{config.BRONZE_DIR}/billing_monthly")
    billing_silver_df = billing_bronze_df \
        .withColumn("subtotal_usd", coalesce(col("subtotal"), lit(0.0)) * col("exchange_rate_to_usd")) \
        .withColumn("credits_usd", coalesce(col("credits"), lit(0.0)) * col("exchange_rate_to_usd")) \
        .withColumn("taxes_usd", coalesce(col("taxes"), lit(0.0)) * col("exchange_rate_to_usd")) \
        .withColumn("net_revenue_usd", (coalesce(col("subtotal"), lit(0.0)) + coalesce(col("taxes"), lit(0.0)) - coalesce(col("credits"), lit(0.0))) * col("exchange_rate_to_usd"))
        
    billing_silver_df.write.mode("overwrite") \
        .parquet(f"{config.SILVER_DIR}/billing_monthly")
    print(f"Silver Billing Monthly count: {billing_silver_df.count()}")

    # 8. Process NPS Surveys in Silver
    print("Processing NPS surveys in Silver...")
    nps_bronze_df = spark.read.parquet(f"{config.BRONZE_DIR}/nps_surveys")
    nps_silver_df = nps_bronze_df.withColumn("nps_score", coalesce(col("nps_score"), lit(0.0)))
    nps_silver_df.write.mode("overwrite") \
        .parquet(f"{config.SILVER_DIR}/nps_surveys")
    print(f"Silver NPS Surveys count: {nps_silver_df.count()}")


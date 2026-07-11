# Gold Layer FinOps Mart Aggregation Module
from pyspark.sql.functions import (
    sum as _sum, round as _round, col, count as _count, avg as _avg, when, lit,
    collect_set, collect_list, struct, map_from_entries
)
from src import config

def process_gold(spark):
    """
    Gold FinOps, Support, and Product/GenAI marts aggregation.
    """
    print("\n--- Phase 4: Gold Marts Creation ---")
    
    # 1. Main FinOps Mart: Daily Usage by Org and Service
    print("Aggregating daily usage metrics by organization and service...")
    silver_events_df = spark.read.parquet(f"{config.SILVER_DIR}/usage_events")
    
    gold_df = silver_events_df.groupBy("org_id", "usage_date", "service") \
        .agg(
            _round(_sum("daily_cost_usd"), 4).alias("total_cost_usd"),
            _sum("requests").alias("total_requests"),
            _round(_sum("cpu_hours"), 4).alias("total_cpu_hours"),
            _round(_sum("storage_gb_hours"), 4).alias("total_storage_gb_hours"),
            _sum("genai_tokens_feat").alias("total_genai_tokens"),
            _round(_sum("carbon_kg_feat"), 6).alias("total_carbon_kg")
        )
    
    gold_df.write.mode("overwrite").parquet(f"{config.GOLD_DIR}/org_daily_usage_by_service")
    print(f"Gold FinOps Mart (org_daily_usage_by_service) count: {gold_df.count()}")
    
    # 2. Monthly Revenue Mart: Revenue by Org and Month
    print("Aggregating monthly revenue by organization...")
    silver_billing_df = spark.read.parquet(f"{config.SILVER_DIR}/billing_monthly")
    
    revenue_df = silver_billing_df.groupBy("org_id", "month") \
        .agg(
            _round(_sum("subtotal_usd"), 4).alias("total_subtotal_usd"),
            _round(_sum("credits_usd"), 4).alias("total_credits_usd"),
            _round(_sum("taxes_usd"), 4).alias("total_taxes_usd"),
            _round(_sum("net_revenue_usd"), 4).alias("net_revenue_usd")
        )
        
    revenue_df.write.mode("overwrite").parquet(f"{config.GOLD_DIR}/revenue_by_org_month")
    print(f"Gold Revenue Mart (revenue_by_org_month) count: {revenue_df.count()}")

    # 3. Support Mart: Tickets by Org and Date
    print("Aggregating daily support ticket metrics...")
    silver_tickets_df = spark.read.parquet(f"{config.SILVER_DIR}/support_tickets")
    
    tickets_df = silver_tickets_df.groupBy("org_id", "ticket_date") \
        .agg(
            _count(lit(1)).alias("total_tickets"),
            _sum(when(col("severity") == "critical", 1).otherwise(0)).alias("critical_tickets_count"),
            _round(_avg(when(col("csat") > 0.0, col("csat")).otherwise(None)), 2).alias("avg_csat"),
            _round(_sum(when(col("sla_breached") == True, 1).otherwise(0)) / _count(lit(1)), 4).alias("sla_breach_rate")
        )
        
    tickets_df.write.mode("overwrite").parquet(f"{config.GOLD_DIR}/tickets_by_org_date")
    print(f"Gold Support Mart (tickets_by_org_date) count: {tickets_df.count()}")

    # 4. Product/Usage Mart: GenAI Tokens by Org and Date
    print("Aggregating daily GenAI tokens usage...")
    genai_df = silver_events_df.filter(col("service") == "genai") \
        .groupBy("org_id", "usage_date") \
        .agg(
            _sum("genai_tokens_feat").alias("total_genai_tokens"),
            _round(_sum("daily_cost_usd"), 4).alias("total_cost_usd")
        )
        
    genai_df.write.mode("overwrite").parquet(f"{config.GOLD_DIR}/genai_tokens_by_org_date")
    print(f"Gold GenAI Mart (genai_tokens_by_org_date) count: {genai_df.count()}")

    # 5. Cost Anomaly Mart: Flagged anomalies
    print("Extracting cost anomalies...")
    anomaly_df = silver_events_df.filter(col("anomaly_flag") == True) \
        .select("org_id", "usage_date", "service", "daily_cost_usd", "anomaly_flag")
        
    anomaly_df.write.mode("overwrite").parquet(f"{config.GOLD_DIR}/cost_anomaly_mart")
    print(f"Gold Cost Anomaly Mart count: {anomaly_df.count()}")

    # 6. Organization Profile Analytics (with collections: set, list, map)
    print("Aggregating organization profiles with collections (roles, comments, costs)...")
    
    users_df = spark.read.parquet(f"{config.BRONZE_DIR}/users")
    roles_df = users_df.filter(col("role").isNotNull()) \
        .groupBy("org_id") \
        .agg(collect_set("role").alias("active_user_roles"))
        
    nps_df = spark.read.parquet(f"{config.SILVER_DIR}/nps_surveys")
    comments_df = nps_df.filter(col("comment").isNotNull() & (col("comment") != "")) \
        .groupBy("org_id") \
        .agg(collect_list("comment").alias("recent_nps_comments"))
        
    org_service_cost = gold_df.groupBy("org_id", "service") \
        .agg(_round(_sum("total_cost_usd"), 4).alias("service_cost"))
        
    costs_map_df = org_service_cost.groupBy("org_id") \
        .agg(map_from_entries(collect_list(struct("service", "service_cost"))).alias("service_accumulated_costs"))
        
    orgs_df = spark.read.parquet(f"{config.BRONZE_DIR}/customers_orgs")
    orgs_base = orgs_df.select("org_id", "org_name", "plan_tier")
    
    profile_df = orgs_base \
        .join(roles_df, "org_id", "left") \
        .join(comments_df, "org_id", "left") \
        .join(costs_map_df, "org_id", "left")
        
    profile_df.write.mode("overwrite").parquet(f"{config.GOLD_DIR}/org_profile_analytics")
    print(f"Gold Organization Profile Mart (org_profile_analytics) count: {profile_df.count()}")

    return gold_df


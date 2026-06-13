# Centralized Configuration for the Cloud Provider Analytics Pipeline
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType,
    DateType, TimestampType, BooleanType
)

# -----------------
# Directory Paths
# -----------------
LANDING_DIR = "datalake/landing"
BRONZE_DIR = "datalake/bronze"
SILVER_DIR = "datalake/silver"
GOLD_DIR = "datalake/gold"
QUARANTINE_DIR = "datalake/quarantine"
CHECKPOINTS_DIR = "checkpoints"

# -----------------
# Master Table Schemas (Bronze Batch)
# -----------------
cust_schema = StructType([
    StructField("org_id", StringType(), False),
    StructField("org_name", StringType(), True),
    StructField("industry", StringType(), True),
    StructField("hq_region", StringType(), True),
    StructField("plan_tier", StringType(), True),
    StructField("is_enterprise", BooleanType(), True),
    StructField("signup_date", DateType(), True),
    StructField("sales_rep", StringType(), True),
    StructField("lifecycle_stage", StringType(), True),
    StructField("marketing_source", StringType(), True),
    StructField("nps_score", DoubleType(), True)
])

users_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("org_id", StringType(), True),
    StructField("email", StringType(), True),
    StructField("role", StringType(), True),
    StructField("active", BooleanType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("last_login", TimestampType(), True)
])

resources_schema = StructType([
    StructField("resource_id", StringType(), False),
    StructField("org_id", StringType(), True),
    StructField("service", StringType(), True),
    StructField("region", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("state", StringType(), True),
    StructField("tags_json", StringType(), True)
])

billing_schema = StructType([
    StructField("invoice_id", StringType(), False),
    StructField("org_id", StringType(), True),
    StructField("month", DateType(), True),
    StructField("subtotal", DoubleType(), True),
    StructField("credits", DoubleType(), True),
    StructField("taxes", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("exchange_rate_to_usd", DoubleType(), True)
])

tickets_schema = StructType([
    StructField("ticket_id", StringType(), False),
    StructField("org_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("created_at", DateType(), True),
    StructField("resolved_at", DateType(), True),
    StructField("csat", DoubleType(), True),
    StructField("sla_breached", BooleanType(), True)
])

nps_schema = StructType([
    StructField("org_id", StringType(), False),
    StructField("survey_date", DateType(), True),
    StructField("nps_score", DoubleType(), True),
    StructField("comment", StringType(), True)
])

marketing_schema = StructType([
    StructField("touch_id", StringType(), False),
    StructField("org_id", StringType(), True),
    StructField("campaign", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("clicked", BooleanType(), True),
    StructField("converted", BooleanType(), True)
])

# -----------------
# Usage Event Stream Schema (Bronze Streaming)
# -----------------
event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("org_id", StringType(), True),
    StructField("resource_id", StringType(), True),
    StructField("service", StringType(), True),
    StructField("region", StringType(), True),
    StructField("metric", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("cost_usd_increment", DoubleType(), True),
    StructField("schema_version", LongType(), True),
    StructField("carbon_kg", DoubleType(), True),
    StructField("genai_tokens", LongType(), True)
])

# -----------------
# Cassandra Settings
# -----------------
CASSANDRA_HOSTS = ['127.0.0.1']
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = "cloud_analytics"
CASSANDRA_TABLE = "org_daily_usage_by_service"
CASSANDRA_TABLE_TICKETS = "tickets_by_org_date"
CASSANDRA_TABLE_REVENUE = "revenue_by_org_month"

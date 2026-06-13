# Bronze Layer Ingestion Module
import os
from pyspark.sql.functions import col, current_timestamp, input_file_name, to_timestamp, lit
from src import config

def process_batch_bronze(spark):
    """
    Ingest master tables from landing CSVs to Parquet partitioned in Bronze, 
    with explicit typing, metadata columns, and deduplication.
    """
    print("\n--- Phase 1: Batch to Bronze ---")
    
    # 1. Customers Orgs
    print("Ingesting customers_orgs.csv...")
    cust_df = spark.read \
        .option("header", "true") \
        .schema(config.cust_schema) \
        .csv(f"{config.LANDING_DIR}/customers_orgs.csv")
        
    cust_df = cust_df.withColumn("ingest_ts", current_timestamp()) \
                     .withColumn("source_file", lit("customers_orgs.csv")) \
                     .dropDuplicates(["org_id"])
    
    cust_df.write.mode("overwrite") \
        .partitionBy("hq_region") \
        .parquet(f"{config.BRONZE_DIR}/customers_orgs")
    print(f"Ingested customers_orgs. Count: {cust_df.count()}")

    # 2. Users
    print("Ingesting users.csv...")
    users_df = spark.read \
        .option("header", "true") \
        .schema(config.users_schema) \
        .csv(f"{config.LANDING_DIR}/users.csv")
        
    users_df = users_df.withColumn("ingest_ts", current_timestamp()) \
                       .withColumn("source_file", lit("users.csv")) \
                       .dropDuplicates(["user_id"])
    
    users_df.write.mode("overwrite") \
        .partitionBy("role") \
        .parquet(f"{config.BRONZE_DIR}/users")
    print(f"Ingested users. Count: {users_df.count()}")

    # 3. Resources
    print("Ingesting resources.csv...")
    res_df = spark.read \
        .option("header", "true") \
        .schema(config.resources_schema) \
        .csv(f"{config.LANDING_DIR}/resources.csv")
        
    res_df = res_df.withColumn("ingest_ts", current_timestamp()) \
                   .withColumn("source_file", lit("resources.csv")) \
                   .dropDuplicates(["resource_id"])
    
    res_df.write.mode("overwrite") \
        .partitionBy("service") \
        .parquet(f"{config.BRONZE_DIR}/resources")
    print(f"Ingested resources. Count: {res_df.count()}")

    # 4. Billing Monthly
    print("Ingesting billing_monthly.csv...")
    billing_df = spark.read \
        .option("header", "true") \
        .schema(config.billing_schema) \
        .csv(f"{config.LANDING_DIR}/billing_monthly.csv")
        
    billing_df = billing_df.withColumn("ingest_ts", current_timestamp()) \
                           .withColumn("source_file", lit("billing_monthly.csv")) \
                           .dropDuplicates(["invoice_id"])
    
    billing_df.write.mode("overwrite") \
        .partitionBy("currency") \
        .parquet(f"{config.BRONZE_DIR}/billing_monthly")
    print(f"Ingested billing_monthly. Count: {billing_df.count()}")

    # 5. Support Tickets
    print("Ingesting support_tickets.csv...")
    tickets_df = spark.read \
        .option("header", "true") \
        .schema(config.tickets_schema) \
        .csv(f"{config.LANDING_DIR}/support_tickets.csv")
        
    tickets_df = tickets_df.withColumn("ingest_ts", current_timestamp()) \
                           .withColumn("source_file", lit("support_tickets.csv")) \
                           .dropDuplicates(["ticket_id"])
                           
    tickets_df.write.mode("overwrite") \
        .partitionBy("category") \
        .parquet(f"{config.BRONZE_DIR}/support_tickets")
    print(f"Ingested support_tickets. Count: {tickets_df.count()}")

    # 6. NPS Surveys
    print("Ingesting nps_surveys.csv...")
    nps_df = spark.read \
        .option("header", "true") \
        .schema(config.nps_schema) \
        .csv(f"{config.LANDING_DIR}/nps_surveys.csv")
        
    nps_df = nps_df.withColumn("ingest_ts", current_timestamp()) \
                   .withColumn("source_file", lit("nps_surveys.csv")) \
                   .dropDuplicates(["org_id", "survey_date"])
                   
    nps_df.write.mode("overwrite") \
        .parquet(f"{config.BRONZE_DIR}/nps_surveys")
    print(f"Ingested nps_surveys. Count: {nps_df.count()}")

    # 7. Marketing Touches
    print("Ingesting marketing_touches.csv...")
    mkt_df = spark.read \
        .option("header", "true") \
        .schema(config.marketing_schema) \
        .csv(f"{config.LANDING_DIR}/marketing_touches.csv")
        
    mkt_df = mkt_df.withColumn("ingest_ts", current_timestamp()) \
                   .withColumn("source_file", lit("marketing_touches.csv")) \
                   .dropDuplicates(["touch_id"])
                   
    mkt_df.write.mode("overwrite") \
        .partitionBy("channel") \
        .parquet(f"{config.BRONZE_DIR}/marketing_touches")
    print(f"Ingested marketing_touches. Count: {mkt_df.count()}")



def process_streaming_bronze(spark):
    """
    Structured Streaming to read events JSONL with explicit schema, watermarks, 
    deduplication by event_id, late data handling, and checkpoints.
    """
    print("\n--- Phase 2: Streaming to Bronze ---")
    
    # Read stream from landing directory
    stream_df = spark.readStream \
        .schema(config.event_schema) \
        .json(f"{config.LANDING_DIR}/usage_events_stream/*.jsonl")
    
    # Parse timestamp and add technical metadata
    parsed_df = stream_df.withColumn("timestamp_parsed", to_timestamp(col("timestamp"))) \
                         .withColumn("ingest_ts", current_timestamp()) \
                         .withColumn("source_file", input_file_name())
    
    # Stateful deduplication over event_id with a 2-hour watermark
    deduped_df = parsed_df \
        .withWatermark("timestamp_parsed", "2 hours") \
        .dropDuplicates(["event_id", "timestamp_parsed"])
    
    # Run streaming query to completion using Trigger AvailableNow
    query = deduped_df.writeStream \
        .format("parquet") \
        .partitionBy("service") \
        .option("checkpointLocation", f"{config.CHECKPOINTS_DIR}/bronze_usage_events") \
        .option("path", f"{config.BRONZE_DIR}/usage_events") \
        .trigger(availableNow=True) \
        .start()
    
    print("Running streaming query to ingest usage events to Bronze...")
    query.awaitTermination()
    print("Streaming Ingestion to Bronze completed successfully.")

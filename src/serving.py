# Cassandra serving layer logic, demo queries, and idempotency checks
from datetime import datetime
from cassandra.cluster import Cluster
from src import config

def serve_to_cassandra(spark, gold_df=None):
    """
    Connect to Cassandra, create schema, and load Gold data.
    """
    print("\n--- Phase 5: Serving in Cassandra (AstraDB/Local) ---")
    
    if gold_df is None:
        gold_df = spark.read.parquet(f"{config.GOLD_DIR}/org_daily_usage_by_service")
        
    rows = gold_df.collect()
    print(f"Connecting to local Cassandra to load {len(rows)} records...")
    
    cluster = Cluster(config.CASSANDRA_HOSTS, port=config.CASSANDRA_PORT)
    session = cluster.connect()
    
    # 1. Create Keyspace
    print(f"Creating keyspace '{config.CASSANDRA_KEYSPACE}'...")
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {config.CASSANDRA_KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
    """)
    session.set_keyspace(config.CASSANDRA_KEYSPACE)
    
    # 2. Create Table
    print(f"Creating query-first table '{config.CASSANDRA_TABLE}'...")
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {config.CASSANDRA_TABLE} (
            org_id text,
            service text,
            usage_date date,
            total_cost_usd double,
            total_requests double,
            total_cpu_hours double,
            total_storage_gb_hours double,
            total_genai_tokens bigint,
            total_carbon_kg double,
            PRIMARY KEY ((org_id), service, usage_date)
        ) WITH CLUSTERING ORDER BY (service ASC, usage_date DESC);
    """)
    
    # 3. Load Data
    insert_stmt = session.prepare(f"""
        INSERT INTO {config.CASSANDRA_TABLE} (
            org_id, service, usage_date, total_cost_usd, total_requests,
            total_cpu_hours, total_storage_gb_hours, total_genai_tokens, total_carbon_kg
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    
    print("Loading records into Cassandra...")
    count = 0
    for row in rows:
        u_date = row['usage_date']
        if isinstance(u_date, str):
            u_date = datetime.strptime(u_date, "%Y-%m-%d").date()
        
        session.execute(insert_stmt, (
            row['org_id'],
            row['service'],
            u_date,
            float(row['total_cost_usd']) if row['total_cost_usd'] is not None else 0.0,
            float(row['total_requests']) if row['total_requests'] is not None else 0.0,
            float(row['total_cpu_hours']) if row['total_cpu_hours'] is not None else 0.0,
            float(row['total_storage_gb_hours']) if row['total_storage_gb_hours'] is not None else 0.0,
            int(row['total_genai_tokens']) if row['total_genai_tokens'] is not None else 0,
            float(row['total_carbon_kg']) if row['total_carbon_kg'] is not None else 0.0
        ))
        count += 1
        
    print(f"Successfully loaded {count} records into Cassandra.")
    cluster.shutdown()


def execute_demo_queries():
    """
    Execute 2 mandatory queries from Cassandra to verify the model.
    Query 1: Costs and daily requests by org and service in a date range.
    Query 2: Top-N services by cumulative cost in the last 14 days for a given organization.
    """
    print("\n--- Phase 6: Executing Demo Queries ---")
    cluster = Cluster(config.CASSANDRA_HOSTS, port=config.CASSANDRA_PORT)
    session = cluster.connect(config.CASSANDRA_KEYSPACE)
    
    # Pick a sample org_id that exists in the database
    res = session.execute(f"SELECT org_id, service, usage_date, total_cost_usd FROM {config.CASSANDRA_TABLE} LIMIT 5;")
    sample_org = None
    sample_service = None
    for r in res:
        sample_org = r.org_id
        sample_service = r.service
        
    if not sample_org:
        print("No data found in Cassandra!")
        cluster.shutdown()
        return
        
    print(f"Sample Organization selected: {sample_org}")
    
    # Query 1: Costos y requests diarios por org y servicio
    print(f"\n[Query 1] Daily costs and requests for Org: '{sample_org}' and Service: '{sample_service}' (last 60 days):")
    query_1_stmt = session.prepare(f"""
        SELECT org_id, service, usage_date, total_cost_usd, total_requests
        FROM {config.CASSANDRA_TABLE}
        WHERE org_id = ? AND service = ? AND usage_date >= '2025-06-01' AND usage_date <= '2025-08-31'
    """)
    rows = session.execute(query_1_stmt, (sample_org, sample_service))
    print(f"{'Date':<12} | {'Service':<12} | {'Total Cost (USD)':<16} | {'Total Requests':<15}")
    print("-" * 65)
    row_count = 0
    for r in rows:
        print(f"{str(r.usage_date):<12} | {r.service:<12} | {r.total_cost_usd:<16.4f} | {r.total_requests:<15.0f}")
        row_count += 1
        if row_count >= 10:
            print("... (showing top 10 rows)")
            break
            
    # Query 2: Top-N servicios por costo acumulado en los últimos 14 días
    print(f"\n[Query 2] Top-N services by cumulative cost for Org: '{sample_org}' in a given range:")
    query_2_stmt = session.prepare(f"""
        SELECT org_id, service, usage_date, total_cost_usd
        FROM {config.CASSANDRA_TABLE}
        WHERE org_id = ?
    """)
    rows = session.execute(query_2_stmt, [sample_org])
    
    service_costs = {}
    for r in rows:
        date_str = str(r.usage_date)
        # Filter range
        if "2025-08-15" <= date_str <= "2025-08-30":
            service_costs[r.service] = service_costs.get(r.service, 0.0) + r.total_cost_usd
            
    # Sort and get Top-N
    sorted_services = sorted(service_costs.items(), key=lambda x: x[1], reverse=True)
    print(f"{'Rank':<5} | {'Service':<15} | {'Cumulative Cost (USD)':<25}")
    print("-" * 50)
    for rank, (serv, cost) in enumerate(sorted_services, 1):
        print(f"{rank:<5} | {serv:<15} | {cost:<25.4f}")
        
    cluster.shutdown()


def test_idempotency(spark):
    """
    Verify that re-running the load doesn't cause record counts to grow (upsert logic).
    """
    print("\n--- Phase 7: Verification of Idempotency ---")
    cluster = Cluster(config.CASSANDRA_HOSTS, port=config.CASSANDRA_PORT)
    session = cluster.connect(config.CASSANDRA_KEYSPACE)
    
    # 1. Count before
    count_before = session.execute(f"SELECT COUNT(*) FROM {config.CASSANDRA_TABLE};").one()[0]
    print(f"Record count in Cassandra before re-running ingestion: {count_before}")
    
    # 2. Re-run
    print("Re-running Gold to Cassandra loading...")
    gold_df = spark.read.parquet(f"{config.GOLD_DIR}/org_daily_usage_by_service")
    serve_to_cassandra(spark, gold_df)
    
    # 3. Count after
    count_after = session.execute(f"SELECT COUNT(*) FROM {config.CASSANDRA_TABLE};").one()[0]
    print(f"Record count in Cassandra after re-running ingestion:  {count_after}")
    
    if count_before == count_after:
        print("SUCCESS: Idempotency OK! Count remains unchanged after re-run.")
    else:
        print("WARNING: Count changed! Check upsert keys.")
        
    cluster.shutdown()

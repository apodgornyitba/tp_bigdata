# Cassandra serving layer logic, demo queries, and idempotency checks
from datetime import datetime
from cassandra.cluster import Cluster
from src import config

def serve_to_cassandra(spark, gold_df=None):
    """
    Connect to Cassandra, create schema, and load Gold data.
    """
    print("\n--- Phase 5: Serving in Cassandra (AstraDB/Local) ---")
    
    # 1. Create Keyspace and Tables from Driver
    cluster = Cluster(config.CASSANDRA_HOSTS, port=config.CASSANDRA_PORT)
    session = cluster.connect()
    
    print(f"Creating keyspace '{config.CASSANDRA_KEYSPACE}'...")
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {config.CASSANDRA_KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
    """)
    session.set_keyspace(config.CASSANDRA_KEYSPACE)
    
    # Table 1: Daily Usage by Org and Service
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
    
    # Table 2: Support Tickets by Org and Date
    print(f"Creating query-first table '{config.CASSANDRA_TABLE_TICKETS}'...")
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {config.CASSANDRA_TABLE_TICKETS} (
            org_id text,
            ticket_date date,
            total_tickets bigint,
            critical_tickets_count bigint,
            avg_csat double,
            sla_breach_rate double,
            PRIMARY KEY ((org_id), ticket_date)
        ) WITH CLUSTERING ORDER BY (ticket_date DESC);
    """)
    
    # Table 3: Monthly Revenue by Org and Month
    print(f"Creating query-first table '{config.CASSANDRA_TABLE_REVENUE}'...")
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {config.CASSANDRA_TABLE_REVENUE} (
            org_id text,
            month date,
            total_subtotal_usd double,
            total_credits_usd double,
            total_taxes_usd double,
            net_revenue_usd double,
            PRIMARY KEY ((org_id), month)
        ) WITH CLUSTERING ORDER BY (month DESC);
    """)
    
    cluster.shutdown()

    # 2. Distributed Load using foreachPartition to executors (Option B)
    
    # A. Ingest org_daily_usage_by_service
    if gold_df is None:
        gold_df = spark.read.parquet(f"{config.GOLD_DIR}/org_daily_usage_by_service")
    print(f"Loading {config.CASSANDRA_TABLE} distributedly via executors (foreachPartition)...")
    
    def load_usage_partition(partition):
        from cassandra.cluster import Cluster
        from datetime import datetime
        cluster = Cluster(config.CASSANDRA_HOSTS, port=config.CASSANDRA_PORT)
        session = cluster.connect(config.CASSANDRA_KEYSPACE)
        insert_stmt = session.prepare(f"""
            INSERT INTO {config.CASSANDRA_TABLE} (
                org_id, service, usage_date, total_cost_usd, total_requests,
                total_cpu_hours, total_storage_gb_hours, total_genai_tokens, total_carbon_kg
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        for row in partition:
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
        cluster.shutdown()

    gold_df.foreachPartition(load_usage_partition)
    
    # B. Ingest tickets_by_org_date
    print(f"Loading {config.CASSANDRA_TABLE_TICKETS} distributedly via executors (foreachPartition)...")
    tickets_gold_df = spark.read.parquet(f"{config.GOLD_DIR}/tickets_by_org_date")
    
    def load_tickets_partition(partition):
        from cassandra.cluster import Cluster
        from datetime import datetime
        cluster = Cluster(config.CASSANDRA_HOSTS, port=config.CASSANDRA_PORT)
        session = cluster.connect(config.CASSANDRA_KEYSPACE)
        insert_stmt = session.prepare(f"""
            INSERT INTO {config.CASSANDRA_TABLE_TICKETS} (
                org_id, ticket_date, total_tickets, critical_tickets_count, avg_csat, sla_breach_rate
            ) VALUES (?, ?, ?, ?, ?, ?)
        """)
        for row in partition:
            t_date = row['ticket_date']
            if isinstance(t_date, str):
                t_date = datetime.strptime(t_date, "%Y-%m-%d").date()
            session.execute(insert_stmt, (
                row['org_id'],
                t_date,
                int(row['total_tickets']) if row['total_tickets'] is not None else 0,
                int(row['critical_tickets_count']) if row['critical_tickets_count'] is not None else 0,
                float(row['avg_csat']) if row['avg_csat'] is not None else 0.0,
                float(row['sla_breach_rate']) if row['sla_breach_rate'] is not None else 0.0
            ))
        cluster.shutdown()

    tickets_gold_df.foreachPartition(load_tickets_partition)

    # C. Ingest revenue_by_org_month
    print(f"Loading {config.CASSANDRA_TABLE_REVENUE} distributedly via executors (foreachPartition)...")
    revenue_gold_df = spark.read.parquet(f"{config.GOLD_DIR}/revenue_by_org_month")
    
    def load_revenue_partition(partition):
        from cassandra.cluster import Cluster
        from datetime import datetime
        cluster = Cluster(config.CASSANDRA_HOSTS, port=config.CASSANDRA_PORT)
        session = cluster.connect(config.CASSANDRA_KEYSPACE)
        insert_stmt = session.prepare(f"""
            INSERT INTO {config.CASSANDRA_TABLE_REVENUE} (
                org_id, month, total_subtotal_usd, total_credits_usd, total_taxes_usd, net_revenue_usd
            ) VALUES (?, ?, ?, ?, ?, ?)
        """)
        for row in partition:
            m_date = row['month']
            if isinstance(m_date, str):
                m_date = datetime.strptime(m_date, "%Y-%m-%d").date()
            session.execute(insert_stmt, (
                row['org_id'],
                m_date,
                float(row['total_subtotal_usd']) if row['total_subtotal_usd'] is not None else 0.0,
                float(row['total_credits_usd']) if row['total_credits_usd'] is not None else 0.0,
                float(row['total_taxes_usd']) if row['total_taxes_usd'] is not None else 0.0,
                float(row['net_revenue_usd']) if row['net_revenue_usd'] is not None else 0.0
            ))
        cluster.shutdown()

    revenue_gold_df.foreachPartition(load_revenue_partition)
    print("Distributed loading to Cassandra completed successfully.")


def execute_demo_queries():
    """
    Execute all 5 mandatory queries from Cassandra to verify the model.
    """
    print("\n--- Phase 6: Executing Demo Queries ---")
    cluster = Cluster(config.CASSANDRA_HOSTS, port=config.CASSANDRA_PORT)
    session = cluster.connect(config.CASSANDRA_KEYSPACE)
    
    # Pick a sample org_id that exists in the database
    res = session.execute(f"SELECT org_id, service FROM {config.CASSANDRA_TABLE} LIMIT 1;")
    sample_org = None
    sample_service = None
    for r in res:
        sample_org = r.org_id
        sample_service = r.service
        
    if not sample_org:
        print("No data found in Cassandra!")
        cluster.shutdown()
        return
        
    print(f"Sample Organization selected for demo queries: {sample_org}")
    
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
    print(f"\n[Query 2] Top-N services by cumulative cost for Org: '{sample_org}' in a given range (15 to 30 of August):")
    query_2_stmt = session.prepare(f"""
        SELECT org_id, service, usage_date, total_cost_usd
        FROM {config.CASSANDRA_TABLE}
        WHERE org_id = ?
    """)
    rows = session.execute(query_2_stmt, [sample_org])
    
    service_costs = {}
    for r in rows:
        date_str = str(r.usage_date)
        if "2025-08-15" <= date_str <= "2025-08-30":
            service_costs[r.service] = service_costs.get(r.service, 0.0) + r.total_cost_usd
            
    sorted_services = sorted(service_costs.items(), key=lambda x: x[1], reverse=True)
    print(f"{'Rank':<5} | {'Service':<15} | {'Cumulative Cost (USD)':<25}")
    print("-" * 50)
    for rank, (serv, cost) in enumerate(sorted_services, 1):
        print(f"{rank:<5} | {serv:<15} | {cost:<25.4f}")
        
    # Query 3: Evolución de tickets críticos y tasa de SLA breach por día (últimos 30 días)
    print(f"\n[Query 3] Critical tickets and SLA breach rate per day for Org: '{sample_org}':")
    query_3_stmt = session.prepare(f"""
        SELECT ticket_date, total_tickets, critical_tickets_count, avg_csat, sla_breach_rate
        FROM {config.CASSANDRA_TABLE_TICKETS}
        WHERE org_id = ? AND ticket_date >= '2025-08-01' AND ticket_date <= '2025-08-31'
    """)
    rows = session.execute(query_3_stmt, [sample_org])
    print(f"{'Date':<12} | {'Total Tickets':<15} | {'Critical Tkts':<15} | {'Avg CSAT':<10} | {'SLA Breach Rate':<15}")
    print("-" * 75)
    row_count = 0
    for r in rows:
        print(f"{str(r.ticket_date):<12} | {r.total_tickets:<15} | {r.critical_tickets_count:<15} | {r.avg_csat:<10.2f} | {r.sla_breach_rate:<15.2%}")
        row_count += 1
        if row_count >= 10:
            print("... (showing top 10 rows)")
            break

    # Query 4: Revenue mensual con créditos/impuestos aplicados (normalizado a USD)
    print(f"\n[Query 4] Monthly billing and net revenue (USD) for Org: '{sample_org}':")
    query_4_stmt = session.prepare(f"""
        SELECT month, total_subtotal_usd, total_credits_usd, total_taxes_usd, net_revenue_usd
        FROM {config.CASSANDRA_TABLE_REVENUE}
        WHERE org_id = ?
    """)
    rows = session.execute(query_4_stmt, [sample_org])
    print(f"{'Month':<12} | {'Subtotal (USD)':<16} | {'Credits (USD)':<15} | {'Taxes (USD)':<13} | {'Net Revenue (USD)':<18}")
    print("-" * 80)
    for r in rows:
        print(f"{str(r.month):<12} | {r.total_subtotal_usd:<16.2f} | {r.total_credits_usd:<15.2f} | {r.total_taxes_usd:<13.2f} | {r.net_revenue_usd:<18.2f}")

    # Query 5: Tokens GenAI y costo estimado por día (si existen)
    print(f"\n[Query 5] GenAI tokens and daily cost for Org: '{sample_org}' (if GenAI used):")
    query_5_stmt = session.prepare(f"""
        SELECT usage_date, total_genai_tokens, total_cost_usd
        FROM {config.CASSANDRA_TABLE}
        WHERE org_id = ? AND service = 'genai'
    """)
    rows = session.execute(query_5_stmt, [sample_org])
    print(f"{'Date':<12} | {'Total GenAI Tokens':<20} | {'Total Cost (USD)':<18}")
    print("-" * 55)
    row_count = 0
    for r in rows:
        print(f"{str(r.usage_date):<12} | {r.total_genai_tokens:<20} | {r.total_cost_usd:<18.4f}")
        row_count += 1
        if row_count >= 10:
            print("... (showing top 10 rows)")
            break
        
    cluster.shutdown()


def test_idempotency(spark):
    """
    Verify that re-running the load doesn't cause record counts to grow (upsert logic).
    """
    print("\n--- Phase 7: Verification of Idempotency ---")
    cluster = Cluster(config.CASSANDRA_HOSTS, port=config.CASSANDRA_PORT)
    session = cluster.connect(config.CASSANDRA_KEYSPACE)
    
    # 1. Counts before
    count_usage_before = session.execute(f"SELECT COUNT(*) FROM {config.CASSANDRA_TABLE};").one()[0]
    count_tickets_before = session.execute(f"SELECT COUNT(*) FROM {config.CASSANDRA_TABLE_TICKETS};").one()[0]
    count_revenue_before = session.execute(f"SELECT COUNT(*) FROM {config.CASSANDRA_TABLE_REVENUE};").one()[0]
    
    print("Record counts before re-running ingestion:")
    print(f" - {config.CASSANDRA_TABLE}: {count_usage_before}")
    print(f" - {config.CASSANDRA_TABLE_TICKETS}: {count_tickets_before}")
    print(f" - {config.CASSANDRA_TABLE_REVENUE}: {count_revenue_before}")
    
    # 2. Re-run
    print("\nRe-running Gold to Cassandra loading...")
    serve_to_cassandra(spark)
    
    # 3. Counts after
    count_usage_after = session.execute(f"SELECT COUNT(*) FROM {config.CASSANDRA_TABLE};").one()[0]
    count_tickets_after = session.execute(f"SELECT COUNT(*) FROM {config.CASSANDRA_TABLE_TICKETS};").one()[0]
    count_revenue_after = session.execute(f"SELECT COUNT(*) FROM {config.CASSANDRA_TABLE_REVENUE};").one()[0]
    
    print("\nRecord counts after re-running ingestion:")
    print(f" - {config.CASSANDRA_TABLE}: {count_usage_after}")
    print(f" - {config.CASSANDRA_TABLE_TICKETS}: {count_tickets_after}")
    print(f" - {config.CASSANDRA_TABLE_REVENUE}: {count_revenue_after}")
    
    # Validate
    if (count_usage_before == count_usage_after and 
        count_tickets_before == count_tickets_after and 
        count_revenue_before == count_revenue_after):
        print("\nSUCCESS: Idempotency OK! All table counts remain unchanged after re-run.")
    else:
        print("\nWARNING: Counts changed! Check Cassandra composite key definitions and upsert logic.")
        
    cluster.shutdown()

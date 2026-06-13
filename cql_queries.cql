-- ==========================================
-- KEYSPACE AND TABLE SETUP (Query-First Model)
-- ==========================================

-- 1. Keyspace Creation
CREATE KEYSPACE IF NOT EXISTS cloud_analytics
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE cloud_analytics;

-- 2. Gold Mart Table: org_daily_usage_by_service
-- This table is designed query-first to answer the FinOps queries (1, 2 and 5):
-- PRIMARY KEY:
-- - Partition Key: org_id (distributes organizations across nodes).
-- - Clustering Keys: service (filters by service) and usage_date (enables range queries).
CREATE TABLE IF NOT EXISTS org_daily_usage_by_service (
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


-- 3. Gold Mart Table: tickets_by_org_date
-- Designed to answer Query 3 (daily support ticket metrics).
-- PRIMARY KEY:
-- - Partition Key: org_id (groups tickets per organization).
-- - Clustering Key: ticket_date (ordered descending to see recent history first).
CREATE TABLE IF NOT EXISTS tickets_by_org_date (
    org_id text,
    ticket_date date,
    total_tickets bigint,
    critical_tickets_count bigint,
    avg_csat double,
    sla_breach_rate double,
    PRIMARY KEY ((org_id), ticket_date)
) WITH CLUSTERING ORDER BY (ticket_date DESC);


-- 4. Gold Mart Table: revenue_by_org_month
-- Designed to answer Query 4 (monthly normalized billing / revenue).
-- PRIMARY KEY:
-- - Partition Key: org_id (groups invoices per organization).
-- - Clustering Key: month (ordered descending to see the latest invoices first).
CREATE TABLE IF NOT EXISTS revenue_by_org_month (
    org_id text,
    month date,
    total_subtotal_usd double,
    total_credits_usd double,
    total_taxes_usd double,
    net_revenue_usd double,
    PRIMARY KEY ((org_id), month)
) WITH CLUSTERING ORDER BY (month DESC);


-- ==========================================
-- MANDATORY DEMO QUERIES (AstraDB / Cassandra)
-- ==========================================

-- Query 1: Costos y requests diarios por org y servicio en un rango de fechas.
-- Example: Show daily cost and requests for organization 'org_c11ertj5' and 'compute' service
-- between 2025-06-01 and 2025-08-31.
SELECT org_id, service, usage_date, total_cost_usd, total_requests
FROM org_daily_usage_by_service
WHERE org_id = 'org_c11ertj5'
  AND service = 'compute'
  AND usage_date >= '2025-06-01'
  AND usage_date <= '2025-08-31';

-- Query 2: Top-N servicios por costo acumulado en los últimos 14 días para una organización.
-- Example: Fetch all records for organization 'org_c11ertj5' to aggregate on the client side
-- for the period 2025-08-15 to 2025-08-30.
SELECT org_id, service, usage_date, total_cost_usd
FROM org_daily_usage_by_service
WHERE org_id = 'org_c11ertj5';

-- Query 3: Evolución de tickets críticos y tasa de SLA breach por día (últimos 30 días).
-- Example: Get ticket metrics for 'org_c11ertj5' in August 2025.
SELECT ticket_date, total_tickets, critical_tickets_count, avg_csat, sla_breach_rate
FROM tickets_by_org_date
WHERE org_id = 'org_c11ertj5'
  AND ticket_date >= '2025-08-01'
  AND ticket_date <= '2025-08-31';

-- Query 4: Revenue mensual con créditos/impuestos aplicados (normalizado a USD).
-- Example: Get monthly invoice data and net revenue for 'org_c11ertj5'.
SELECT month, total_subtotal_usd, total_credits_usd, total_taxes_usd, net_revenue_usd
FROM revenue_by_org_month
WHERE org_id = 'org_c11ertj5';

-- Query 5: Tokens GenAI y costo estimado por día (si existen).
-- Example: Get GenAI token counts and estimated costs per day for organization 'org_c11ertj5'
SELECT org_id, service, usage_date, total_genai_tokens, total_cost_usd
FROM org_daily_usage_by_service
WHERE org_id = 'org_c11ertj5'
  AND service = 'genai';


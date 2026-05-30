-- ==========================================
-- KEYSACE AND TABLE SETUP (Query-First Model)
-- ==========================================

-- 1. Keyspace Creation
CREATE KEYSPACE IF NOT EXISTS cloud_analytics
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE cloud_analytics;

-- 2. Gold Mart Table: org_daily_usage_by_service
-- This table is designed query-first to answer the two main FinOps queries:
-- - Query 1: Costs and daily requests by organization and service in a date range.
-- - Query 2: Top-N services by cumulative cost in the last 14 days for a given organization.
--
-- Primary Key Structure:
-- Partition Key: org_id (distributes organizations across the Cassandra cluster).
-- Clustering Keys: service (allows filtering by service) and usage_date (allows range queries).
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
-- Example: Fetch all records for organization 'org_c11ertj5' and aggregate in client/Python side
-- to compute top-N services during a 14-day window (e.g. 2025-08-15 to 2025-08-30).
SELECT org_id, service, usage_date, total_cost_usd
FROM org_daily_usage_by_service
WHERE org_id = 'org_c11ertj5';

-- Query 3: Evolución de tickets críticos y tasa de SLA breach por día (últimos 30 días).
-- Note: This is an analytical query related to the support tickets mart.
-- Keyspace / Table modeling:
-- Primary Key: ((org_id), ticket_date)
-- Query Example:
-- SELECT ticket_date, critical_tickets_count, sla_breach_rate FROM support_tickets_by_org_date WHERE org_id = 'org_c11ertj5' AND ticket_date >= '2025-08-01';

-- Query 4: Revenue mensual con créditos/impuestos aplicados (normalizado a USD).
-- Note: Calculated from the billing_monthly table, normalising: subtotal + taxes - credits (converted to USD using exchange_rate_to_usd).
-- Query Example:
-- SELECT org_id, month, (subtotal + taxes - credits) * exchange_rate_to_usd AS net_revenue_usd FROM billing_monthly_bronze WHERE org_id = 'org_c11ertj5';

-- Query 5: Tokens GenAI y costo estimado por día (si existen).
-- Example: Get GenAI token counts and estimated costs per day for organization 'org_c11ertj5'
SELECT org_id, service, usage_date, total_genai_tokens, total_cost_usd
FROM org_daily_usage_by_service
WHERE org_id = 'org_c11ertj5'
  AND service = 'genai';

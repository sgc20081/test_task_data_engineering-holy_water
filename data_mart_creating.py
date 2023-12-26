from google.cloud import bigquery
from google.oauth2 import service_account

from decouple import config

import bigquery_settings as BG_S

client = bigquery.Client(project=BG_S.holly_water_project_id, credentials=BG_S.holly_water_credentials)

cpi_query = f"""
    DROP VIEW IF EXISTS {BG_S.InstallsBQSettings.dataset_id}.cpi_data_mart;

    CREATE OR REPLACE VIEW {BG_S.InstallsBQSettings.dataset_id}.cpi_data_mart AS
        WITH 
            InstallData AS (
                SELECT
                    DATE(install_time) AS install_date,
                    COUNT(*) AS installs_count
                FROM 
                    {BG_S.InstallsBQSettings.dataset_id}.{BG_S.InstallsBQSettings.table_id}
                WHERE 
                    DATE(install_time) BETWEEN DATE('2023-12-13') AND DATE('2023-12-15')
                GROUP BY
                    install_date
                ORDER BY
                    install_date
                ),
            CostData AS (
                SELECT
                    SUM(cost) AS total_cost
                FROM 
                    {BG_S.CostsBQSettings.dataset_id}.{BG_S.CostsBQSettings.table_id}
                )
        SELECT
            install_date,
            installs_count,
            total_cost,
        IF(installs_count > 0, total_cost / installs_count, NULL) AS cpi
        FROM 
            InstallData, CostData;
    
    SELECT * FROM {BG_S.InstallsBQSettings.dataset_id}.cpi_data_mart;
    """

revenue_query = f"""
    DROP VIEW IF EXISTS {BG_S.OrdersBQSettings.dataset_id}.revenue_data_mart;

    CREATE OR REPLACE VIEW {BG_S.OrdersBQSettings.dataset_id}.revenue_data_mart AS
        WITH
            OrdersData AS (
                SELECT
                    DATE(event_time) AS orders_date,
                    SUM(iap_item_price) - SUM(discount_amount) - SUM(fee) - SUM(tax) AS revenue
                FROM 
                    {BG_S.OrdersBQSettings.dataset_id}.{BG_S.OrdersBQSettings.table_id}
                WHERE
                    DATE(event_time) BETWEEN DATE('2023-12-13') AND DATE('2023-12-15')
                GROUP BY
                    orders_date
                ORDER BY
                    orders_date
                )
        SELECT
            orders_date,
            revenue
        FROM 
            OrdersData;

    SELECT * FROM {BG_S.OrdersBQSettings.dataset_id}.revenue_data_mart;
"""

roas_query = f"""
    DROP VIEW IF EXISTS {BG_S.OrdersBQSettings.dataset_id}.roas_data_mart;

    CREATE OR REPLACE VIEW {BG_S.OrdersBQSettings.dataset_id}.roas_data_mart AS
        WITH
            OrdersData AS (
                SELECT
                    DATE(event_time) AS orders_date,
                    SUM(iap_item_price) - SUM(discount_amount) - SUM(fee) - SUM(tax) AS revenue
                FROM 
                    {BG_S.OrdersBQSettings.dataset_id}.{BG_S.OrdersBQSettings.table_id}
                WHERE
                    DATE(event_time) BETWEEN DATE('2023-12-13') AND DATE('2023-12-15')
                GROUP BY
                    orders_date
                ORDER BY
                    orders_date
                ),
            CostData AS (
                SELECT
                    SUM(cost) AS total_cost
                FROM 
                    {BG_S.CostsBQSettings.dataset_id}.{BG_S.CostsBQSettings.table_id}
                )
        SELECT
            orders_date,
            IF(total_cost > 0, revenue / total_cost, NULL) AS roas
        FROM 
            OrdersData, CostData;

    SELECT * FROM {BG_S.OrdersBQSettings.dataset_id}.roas_data_mart;
"""

query_job = client.query(roas_query)
results = query_job.result()  # Ждем завершения выполнения запроса
print(results)
for row in results:
    print(row)
from datetime import datetime

from google.cloud import bigquery
from google.oauth2 import service_account

from decouple import config

import bigquery_settings as BG_S

log_datetime = datetime.now()

client = bigquery.Client(project=BG_S.holly_water_project_id, credentials=BG_S.holly_water_credentials)

query = f"""
    DROP TABLE IF EXISTS {BG_S.InstallsBQSettings.dataset_id}.{BG_S.InstallsBQSettings.table_id};

    DROP TABLE IF EXISTS {BG_S.OrdersBQSettings.dataset_id}.{BG_S.OrdersBQSettings.table_id};

    DROP TABLE IF EXISTS {BG_S.OrdersBQSettings.dataset_id}.{BG_S.OrdersBQSettings.table_id};
"""

query_job = client.query(query)
results = query_job.result()

print(f'<{log_datetime}>: Базы данных удалены')

# Ждем завершения выполнения запроса
# print(results)
# for row in results:
#     print(row)
from google.cloud import bigquery
from google.oauth2 import service_account

from decouple import config

json_key = config('GOOGLE_BIGQUERY_JSON_KEY')

holly_water_project_id = 'test-task-data-manager'
holly_water_credentials = service_account.Credentials.from_service_account_file(json_key)

class InstallsBQSettings:

    dataset_id = 'holly_water_api'
    table_id = 'installs_table'

class OrdersBQSettings:

    dataset_id = 'holly_water_api'
    table_id = 'orders_table'

class CostsBQSettings:

    dataset_id = 'holly_water_api'
    table_id = 'costs_table'

class EventsBQSettings:

    dataset_id = 'holly_water_api'
    table_id = 'events_table'
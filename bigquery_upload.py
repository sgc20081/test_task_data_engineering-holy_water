from google.cloud import bigquery
from google.oauth2 import service_account

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

from decouple import config

import bigquery_settings as BG_S

class BigQueryUploadData:

    project_id = None
    dataset_id = None
    table_id = None

    credentials = None
    schema = None

    def __init__(self, data: list):
        self.client = bigquery.Client(project=self.project_id, credentials=self.credentials)
        self.get_data_frame(data)

    def get_data_frame(self, data: list):

        df = pd.DataFrame(data)

        dataset = self.client.dataset(self.dataset_id)
        print('Начало процесса загрузки данных в dataset', dataset)
        try:
            self.client.get_dataset(self.dataset_id)
            print(f"Dataset {self.dataset_id} существует.")
        except Exception as e:
            print(f"Dataset {self.dataset_id} не существует. Ошибка: {e}")
            print('Создаётся новый dataset')
            self.client.create_dataset(dataset)
        
        self.bigquery_upload_data(df)

    def bigquery_upload_data(self, df):

        table_ref = self.client.dataset(self.dataset_id).table(self.table_id)
        
        print('Подключение создано')
        
        job_config = bigquery.LoadJobConfig(
            schema=self.schema,
            write_disposition="WRITE_APPEND",  
            # "WRITE_APPEND", если нужно добавить данные к уже существующим в таблице
            # "WRITE_TRUNCATE", если нужно перезаписать старые данные на новые данные из запроса
        )
        
        print('Записи созданы')
        
        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        
        print('Данные загружены в BigQuery')
        
        job.result()  # Ждем завершения задачи загрузки


class InstallsBigQueryUploadData(BigQueryUploadData):
    
    project_id = BG_S.holly_water_project_id
    credentials = BG_S.holly_water_credentials
    
    dataset_id = BG_S.InstallsBQSettings.dataset_id
    table_id = BG_S.InstallsBQSettings.table_id
    
    schema = [
        bigquery.SchemaField('install_time', 'DATETIME'),
        bigquery.SchemaField('marketing_id', 'STRING'),
        bigquery.SchemaField('channel', 'STRING'),
        bigquery.SchemaField('medium', 'STRING'),
        bigquery.SchemaField('campaign', 'STRING'),
        bigquery.SchemaField('keyword', 'STRING'),
        bigquery.SchemaField('ad_content', 'STRING'),
        bigquery.SchemaField('ad_group', 'STRING'),
        bigquery.SchemaField('landing_page', 'STRING'),
        bigquery.SchemaField('sex', 'STRING'),
        bigquery.SchemaField('alpha_2', 'STRING'),
        bigquery.SchemaField('alpha_3', 'STRING'),
        bigquery.SchemaField('flag', 'STRING'),
        bigquery.SchemaField('name', 'STRING'),
        bigquery.SchemaField('numeric', 'STRING'),
        bigquery.SchemaField('official_name', 'STRING')
    ]

class OredersBigQueryUploadData(BigQueryUploadData):

    project_id = BG_S.holly_water_project_id
    credentials = BG_S.holly_water_credentials
    
    dataset_id = BG_S.OrdersBQSettings.dataset_id
    table_id = BG_S.OrdersBQSettings.table_id
    
    schema = [
        bigquery.SchemaField('event_time', 'DATETIME'),
        bigquery.SchemaField('transaction_id', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('type', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('origin_transaction_id', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('category', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('payment_method', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('fee', 'DECIMAL', precision=20, scale=2, mode='NULLABLE'),
        bigquery.SchemaField('tax', 'DECIMAL', precision=20, scale=2, mode='NULLABLE'),
        bigquery.SchemaField('iap_item_name', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('iap_item_price', 'DECIMAL', precision=20, scale=2, mode='NULLABLE'),
        bigquery.SchemaField('discount_code', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('discount_amount', 'DECIMAL', precision=20, scale=2, mode='NULLABLE'),
    ]

class CostsBigQueryUploadData(BigQueryUploadData):
    
    project_id = BG_S.holly_water_project_id
    credentials = BG_S.holly_water_credentials
    
    dataset_id = BG_S.CostsBQSettings.dataset_id
    table_id = BG_S.CostsBQSettings.table_id
    
    schema = [
        bigquery.SchemaField('landing_page', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('keyword', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('channel', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('medium', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('ad_content', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('ad_group', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('location', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('campaign', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('cost', 'DECIMAL', precision=20, scale=2, mode='NULLABLE'),
    ]

class EventsBigQueryUploadData(BigQueryUploadData):
    
    project_id = BG_S.holly_water_project_id
    credentials = BG_S.holly_water_credentials
    
    dataset_id = BG_S.EventsBQSettings.dataset_id
    table_id = BG_S.EventsBQSettings.table_id
    
    schema = [
        bigquery.SchemaField('user_id', 'STRING'),
        bigquery.SchemaField('event_time', 'DATETIME'),
        bigquery.SchemaField('alpha_2', 'STRING'),
        bigquery.SchemaField('alpha_3', 'STRING'),
        bigquery.SchemaField('flag', 'STRING'),
        bigquery.SchemaField('name', 'STRING'),
        bigquery.SchemaField('numeric', 'STRING'),
        bigquery.SchemaField('official_name', 'STRING'),
        bigquery.SchemaField('os', 'STRING'),
        bigquery.SchemaField('brand', 'STRING'),
        bigquery.SchemaField('model', 'STRING'),
        bigquery.SchemaField('model_number', 'INTEGER'),
        bigquery.SchemaField('specification', 'STRING'),
        bigquery.SchemaField('event_type', 'STRING'),
        bigquery.SchemaField('location', 'STRING'),
        bigquery.SchemaField('user_action_detail', 'STRING'),
        bigquery.SchemaField('session_number', 'INTEGER'),
        bigquery.SchemaField('localization_id', 'STRING'),
        bigquery.SchemaField('ga_session_id', 'STRING'),
        bigquery.SchemaField('value', 'DECIMAL', precision=20, scale=1),
        bigquery.SchemaField('state', 'DECIMAL', precision=20, scale=1),
        bigquery.SchemaField('engagement_time_msec', 'DECIMAL', precision=20, scale=1),
        bigquery.SchemaField('current_progress', 'STRING'),
        bigquery.SchemaField('event_origin', 'STRING'),
        bigquery.SchemaField('place', 'DECIMAL', precision=20, scale=1),
        bigquery.SchemaField('selection', 'BOOLEAN'),
        bigquery.SchemaField('analytics_storage', 'STRING'),
        bigquery.SchemaField('browser', 'STRING'),
        bigquery.SchemaField('install_store', 'STRING'),
        bigquery.SchemaField('transaction_id', 'STRING'),
        bigquery.SchemaField('campaign_name', 'STRING'),
        bigquery.SchemaField('source', 'STRING'),
        bigquery.SchemaField('medium', 'STRING'),
        bigquery.SchemaField('term', 'STRING'),
        bigquery.SchemaField('context', 'STRING'),
        bigquery.SchemaField('gclid', 'STRING'),
        bigquery.SchemaField('dclid', 'STRING'),
        bigquery.SchemaField('srsltid', 'STRING'),
        bigquery.SchemaField('is_active_user', 'STRING'),
        bigquery.SchemaField('marketing_id', 'STRING'),
    ]
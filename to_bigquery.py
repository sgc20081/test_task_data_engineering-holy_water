import time

import schedule

from decouple import config

# Внутрениие модули проекта
from api_requests import *
from utils import GetProperties, getparam_yesterday_date

api_url = config('API_URL')
api_key = config('API_KEY')

def from_api_to_bigquery():

    yester_date = getparam_yesterday_date()

    InstallsAPIRequest(
                    url=api_url, 
                    headers={'Authorization': api_key}, 
                    api_method='installs', 
                    params={'date': yester_date})

    OrdersAPIRequest(
                    url=api_url, 
                    headers={'Authorization': api_key}, 
                    api_method='orders', 
                    params={'date': yester_date})

    CostsAPIRequest(
                    url=api_url, 
                    headers={'Authorization': api_key}, 
                    api_method='costs', 
                    params={'date': yester_date, 'dimensions': 'location,channel,medium,campaign,keyword,ad_content,ad_group,landing_page'})

    EventsAPIRequest(
                    url=api_url, 
                    headers={'Authorization': api_key}, 
                    api_method='events', 
                    params={'date': yester_date})

schedule.every().day.at('02:00').do(from_api_to_bigquery)

while True:
    schedule.run_pending()
    time.sleep(3600)

"""
project_id = 'test-task-data-manager'
credentials = service_account.Credentials.from_service_account_file(
    'test-task-data-manager-a8796afc73ae.json')

client = bigquery.Client(project=project_id, credentials=credentials)

data = [
    {"column1": 'value1', "column2": 'value2'},
    {"column1": 'value3', "column2": 'value4'},
    # Другие словари
]

df = pd.DataFrame(data)

dataset_id = "data_test"
table_id = "data_test_table"

dataset = client.dataset(dataset_id)

if dataset is None:
    print('Создаётся новый датасет')
    client.create_dataset(dataset)

    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("column1", "STRING"),
            bigquery.SchemaField("column2", "STRING"),
            # Добавьте другие поля и их типы данных
        ],
        # write_disposition="WRITE_TRUNCATE",  # Замените на "WRITE_APPEND", если вы хотите добавить данные
        write_disposition="WRITE_APPEND",  # Замените на "WRITE_APPEND", если вы хотите добавить данные
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Ждем завершения задачи загрузки
else:
    print('Датасет уже есть')
    table_ref = client.dataset(dataset_id).table(table_id)
    print('Подключение создано')
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("column1", "STRING"),
            bigquery.SchemaField("column2", "STRING"),
            # Добавьте другие поля и их типы данных
        ],
        # write_disposition="WRITE_TRUNCATE",  # Замените на "WRITE_APPEND", если вы хотите добавить данные
        write_disposition="WRITE_APPEND",  # Замените на "WRITE_APPEND", если вы хотите добавить данные
    )
    print('Записи созданы')
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    print('Данные загружены в BigQuery')
    job.result()  # Ждем завершения задачи загрузки
"""

# Проблема заключается в том, что при создании схемы данных (ИМЕННО СОЗДАНИИ) BigQuery интерпретирует
# значения None как численные NULL и определяет тип данных столбца как INTEGER (даже если было указано STRING).
# Соответственно, данные типа str больше не могут быть загружены в эти столбцы.
# В качестве решения при развёртке данных, попробовать заменять значения None на str(None), если в данном столбце ожидается строка, а не чиленное значение
# test_data_list = [
#                 {'user_id': 'nPyK3AMVav9ZEOaY', 'event_time': '2023-12-12 00:03:18', 'alpha_2': 'AT', 'alpha_3': 'AUT', 'flag': '🇦🇹', 'name': 'Austria', 'numeric': '040', 'official_name': 'Republic of Austria', 'os': 'iOS', 'brand': 'Apple', 'model': 'iPhone', 'model_number': 14, 'specification': '', 'event_type': 'Search Initiated', 'location': '', 'user_action_detail': '', 'session_number': None, 'localization_id': 'es', 'ga_session_id': None, 'value': Decimal('76.0'), 'state': Decimal('0.0'), 'engagement_time_msec': Decimal('47360.0'), 'current_progress': None, 'event_origin': 'auto', 'place': Decimal('8.0'), 'selection': None, 'analytics_storage': 'Yes', 'browser': None, 'install_store': 'Null', 'transaction_id': None, 'campaign_name': None, 'source': None, 'medium': None, 'term': None, 'context': None, 'gclid': None, 'dclid': None, 'srsltid': None, 'user_is_active': None, 'marketing_id': 'YaAelMIuE0vC'},
#                 {'user_id': 'nPyK3AMVav9ZEOaY', 'event_time': '2023-12-12 00:03:18', 'alpha_2': 'AT', 'alpha_3': 'AUT', 'flag': '🇦🇹', 'name': 'Austria', 'numeric': '040', 'official_name': 'Republic of Austria', 'os': 'iOS', 'brand': 'Apple', 'model': 'iPhone', 'model_number': 14, 'specification': '', 'event_type': 'Search Initiated', 'location': '', 'user_action_detail': '', 'session_number': 'test', 'localization_id': 'es', 'ga_session_id': 'test', 'value': Decimal('76.0'), 'state': Decimal('0.0'), 'engagement_time_msec': Decimal('47360.0'), 'current_progress': 'test', 'event_origin': 'auto', 'place': Decimal('8.0'), 'selection': 'test', 'analytics_storage': 'Yes', 'browser': 'test', 'install_store': 'Null', 'transaction_id': 'test', 'campaign_name': 'test', 'source': 'test', 'medium': 'test', 'term': 'test', 'context': 'test', 'gclid': 'test', 'dclid': 'test', 'srsltid': 'test', 'user_is_active': 'test', 'marketing_id': 'YaAelMIuE0vC'},
#                 ]
# for ind in test_data_list:
#     for field, value in ind.items():
#         print(f"{field}: {value}, {type(value)}")
#     print('===================================')
# test_events_send_bugrequest = EventsBigQueryUploadData(test_data_list)
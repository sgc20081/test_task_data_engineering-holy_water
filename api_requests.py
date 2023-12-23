import json
import io

import requests

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

from bigquery_upload import *
from utils import DataConversion

class APIRequest:
    
    def __init__(self,
                 url: str,
                 headers: dict | None = ...,
                 params: dict | None = ...,
                 api_method: str | None = ...,
                #  obj_model: models.Model | None = ...
                ):
        
        self.url = url
        self.headers = headers
        self.params = params
        self.api_method = api_method
        # self.obj_model = obj_model

        self.make_api_request()

    def make_api_request(self):

        if self.api_method != None:
            self.url += self.api_method

        try:
            response = requests.get(self.url, headers=self.headers, params=self.params)
            response.raise_for_status()
            self.api_response(response)
        except requests.exceptions.RequestException as e:
            print(f"Error making API request: {e}")

    # Используется для вывода результата в дочернем классе
    def api_response(self, response):
        pass

class InstallsAPIRequest(APIRequest):

    schema = {
        'install_time': 'DATETIME',
        'marketing_id': 'STRING',
        'channel': 'STRING',
        'medium': 'STRING',
        'campaign': 'STRING',
        'keyword': 'STRING',
        'ad_content': 'STRING',
        'ad_group': 'STRING',
        'landing_page': 'STRING',
        'sex': 'STRING',
        'alpha_2': 'STRING',
        'alpha_3': 'STRING',
        'flag': 'STRING',
        'name': 'STRING',
        'numeric': 'STRING',
        'official_name': 'STRING'
    }

    def api_response(self, response):
        data = json.loads(response.content)

        for key, val in data.items():
            
            if isinstance(val, str):
                try:
                    val = json.loads(val)
                except json.decoder.JSONDecodeError as e:
                    print(f'Error: {e}\nData: {val}')
            
            if isinstance(val, list):

                for ind, dict_content in enumerate(val):
                    for field, value in dict_content.items():
                            val[ind][field] = DataConversion.make_bigquery_valid_data(self.schema, field, value)
                    
                InstallsBigQueryUploadData(val)
                # objects_list = []

                # for record in val:
                #     obj = Installs(**record)
                #     objects_list.append(obj)

                # Installs.objects.bulk_create(objects_list)S

class OrdersAPIRequest(APIRequest):

    schema = {
        'event_time': 'DATETIME',
        'transaction_id': 'STRING',
        'type': 'STRING',
        'origin_transaction_id': 'STRING',
        'category': 'STRING',
        'payment_method': 'STRING',
        'fee': 'DECIMAL',
        'tax': 'DECIMAL',
        'iap_item_name': 'STRING',
        'iap_item_price': 'DECIMAL',
        'discount_code': 'STRING',
        'discount_amount': 'DECIMAL',
    }

    def api_response(self, response):

        table = pq.read_table(io.BytesIO(response.content))
        tb_dict = table.to_pydict()
        to_db_list = []
        
        for field, value in tb_dict.items():
            
            if '.' in field:
                field = field.replace('.', '_')

            for i, content in enumerate(value):
                
                content = DataConversion.make_bigquery_valid_data(self.schema, field, content)
                
                if 0 <= i < len(to_db_list):
                    to_db_list[i].update({field: content})
                else:
                    to_db_list.append({field: content})
        
        OredersBigQueryUploadData(to_db_list)
        # objects_list = []

        # for order in to_db_list:
        #     obj = Orders(**order)
        #     objects_list.append(obj)

        # Orders.objects.bulk_create(objects_list)

class CostsAPIRequest(APIRequest):

    schema = {
        'landing_page': 'STRING',
        'keyword': 'STRING',
        'channel': 'STRING',
        'medium': 'STRING',
        'ad_content': 'STRING',
        'ad_group': 'STRING',
        'location': 'STRING',
        'campaign': 'STRING',
        'cost': 'DECIMAL'
    }

    def api_response(self, response):
        data = response.content.decode('utf-8')
        data = data.replace('\t', ',')
        data_dict = data.split('\n')
        
        fields = []
        to_db_list = []

        for data_dict_ind, data_dict_val in enumerate(data_dict):
            
            data_dict_val = data_dict_val.split(',')

            if data_dict_val == ['']:
                continue
            
            if data_dict_ind == 0:
                fields = data_dict_val
            else:
                for i, value in enumerate(data_dict_val):
                    
                    value = DataConversion.make_bigquery_valid_data(self.schema, fields[i], value)

                    if to_db_list == [] or not 0 <= data_dict_ind-1 < len(to_db_list):
                        to_db_list.append({fields[i]: value})
                    else:
                        to_db_list[data_dict_ind-1].update({fields[i]: value})

        CostsBigQueryUploadData(to_db_list)

        # objects_list = []

        # for cost in to_db_list:
        #     obj = Costs(**cost)
        #     objects_list.append(obj)
        
        # Costs.objects.bulk_create(objects_list)

class EventsAPIRequest(APIRequest):

    schema = {
        'user_id': 'STRING',
        'event_time': 'DATETIME',
        'alpha_2': 'STRING',
        'alpha_3': 'STRING',
        'flag': 'STRING',
        'name': 'STRING',
        'numeric': 'STRING',
        'official_name': 'STRING',
        'os': 'STRING',
        'brand': 'STRING',
        'model': 'STRING',
        'model_number': 'INTEGER',
        'specification': 'STRING',
        'event_type': 'STRING',
        'location': 'STRING',
        'user_action_detail': 'STRING',
        'session_number': 'INTEGER',
        'localization_id': 'STRING',
        'ga_session_id': 'STRING',
        'value': 'DECIMAL',
        'state': 'DECIMAL',
        'engagement_time_msec': 'DECIMAL',
        'current_progress': 'STRING',
        'event_origin': 'STRING',
        'place': 'DECIMAL',
        'selection': 'BOOLEAN',
        'analytics_storage': 'STRING',
        'browser': 'STRING',
        'install_store': 'STRING',
        'transaction_id': 'STRING',
        'campaign_name': 'STRING',
        'source': 'STRING',
        'medium': 'STRING',
        'term': 'STRING',
        'context': 'STRING',
        'gclid': 'STRING',
        'dclid': 'STRING',
        'srsltid': 'STRING',
        'is_active_user': 'STRING',
        'marketing_id': 'STRING',
    }

    def api_response(self, response):
        data = json.loads(response.content.decode('utf-8'))

        for key, val in data.items():

            if key != 'next_page':
                val = json.loads(val)
                
                # foreign_objects_list = []
                # objects_list = []
                to_db_list = []

                for user_event in val:
                    obj_dict = {}
                    
                    for field, value in user_event.items():
                        
                        value = DataConversion.make_bigquery_valid_data(self.schema, field, value)

                        if not isinstance(value, dict):
                            obj_dict[field] = value
                        else:
                            for foreign_field, foreign_value in value.items():
                                foreign_value = DataConversion.make_bigquery_valid_data(self.schema, foreign_field, foreign_value)
                                obj_dict[foreign_field] = foreign_value
                    
                    to_db_list.append(obj_dict)
                    
                    # obj = Events(**obj_dict)
                    # objects_list.append(obj)
                
                EventsBigQueryUploadData(to_db_list)

                # UserParams.objects.bulk_create(foreign_objects_list)
                # Events.objects.bulk_create(objects_list)
        
            elif key == 'next_page':
                self.params['next_page'] = val
                print('Пошёл запрос следующей страницы')
                self.make_api_request()
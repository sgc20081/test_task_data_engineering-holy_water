import time
import datetime
from decimal import Decimal

class GetProperties:

    def __init__(
                self,
                file_path: str,):
        
        file = open(file_path, 'r')
        content_list = file.read().splitlines()
        file.close()
        
        for prop in content_list:
            key, value = prop.split(' = ')
            setattr(self, key, value)

class DataConversion:

    @classmethod
    def str_to_datetime(self, date: str):
        try:
            # Строка с микросекундами
            return datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            # Если не удалось, строку без микросекунд
            return datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")
    
    @classmethod
    def str_to_decimal(self, string: str):
        try:
            return Decimal(string)
        except TypeError:
            print(f"{self.__class__.__name__}.str_to_decimal awaits number as a string, not {string}")

    @classmethod
    def float_to_decimal(self, float: float):
        return Decimal(str(float))

    @classmethod
    def str_to_decimal(self, string: str):
        return Decimal(string)
    
    @classmethod
    def int_to_datetime(self, integer: int):
        timestamp_seconds = integer / 1000.0
        datetime_obj = datetime.datetime.fromtimestamp(timestamp_seconds)
        # return datetime_obj.strftime('%Y-%m-%d %H:%M:%S')
        return datetime_obj

    @classmethod
    def _bigquery_schema_str_(self, value):
        if value is None or value == '':
            return str(None)
        else:
            return str(value)

    @classmethod
    def _bigquery_schema_datetime_(self, value):
        if isinstance(value, str):
            return self.str_to_datetime(value)
        elif isinstance(value, int):
            return self.int_to_datetime(value)
        else:
            return value

    @classmethod
    def _bigquery_schema_decimal_(self, value):
        if isinstance(value, float):
            return self.float_to_decimal(value)
        if isinstance(value, str):
            return self.str_to_decimal(value)

    @classmethod
    def make_bigquery_valid_data(self, schema: dict, field, value):
        if field in schema:

            if schema[field] == 'STRING':
                return self._bigquery_schema_str_(value)
            elif schema[field] == 'INTEGER' and value == '':
                return None
            elif schema[field] == 'DECIMAL':
                return self._bigquery_schema_decimal_(value)
            elif schema[field] == 'BOOLEAN' and value == None:
                return False
            elif schema[field] == 'DATETIME':
                return self._bigquery_schema_datetime_(value)
            else:
                return value
        else:
            return value
        
def getparam_yesterday_date():
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    get_param_date = yesterday.strftime('%Y-%m-%d')
    return get_param_date